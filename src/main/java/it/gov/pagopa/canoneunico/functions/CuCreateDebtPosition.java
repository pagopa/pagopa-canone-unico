package it.gov.pagopa.canoneunico.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.model.PaymentOptionModel;
import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import it.gov.pagopa.canoneunico.model.RetryStep;
import it.gov.pagopa.canoneunico.model.Transfer;
import it.gov.pagopa.canoneunico.service.DebtPositionQueueService;
import it.gov.pagopa.canoneunico.service.DebtPositionTableService;
import it.gov.pagopa.canoneunico.service.GpdClient;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Azure Functions with Azure Queue trigger.
 */
public class CuCreateDebtPosition {

    private final String maxAttempts = System.getenv("MAX_ATTEMPTS");


    /**
     * This function will be invoked when a new message is detected in the queue
     */
    @FunctionName("CuCreateDebtPositionFunction")
    public void run(
            @QueueTrigger(name = "DebtPositionTrigger", queueName = "%DEBT_POSITIONS_QUEUE%", connection = "CU_SA_CONNECTION_STRING") String message,
            final ExecutionContext context) {

        Logger logger = context.getLogger();
        logger.log(Level.CONFIG, () -> "[CuCreateDebtPositionFunction][id=" + context.getInvocationId() + "] new message " + message);

        try {
            // map message in a model
            var debtPositions = new ObjectMapper().readValue(message, DebtPositionMessage.class);
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction START][id=" + context.getInvocationId() + "] create debt position of the file: " + debtPositions.getCsvFilename());


            // in parallel, for each element in the message calls GPD for the status and updates the elem status in the table
            long startTime = System.currentTimeMillis();

            var failed = debtPositions.getRows()
                    .parallelStream()
                    .filter(row -> !RetryStep.DONE.equals(createAndPublishDebtPosition(debtPositions.getCsvFilename(), logger, row, context.getInvocationId())))
                    .collect(Collectors.toList());

            long endTime = System.currentTimeMillis();
            logger.log(Level.INFO, () -> String.format("[CuCreateDebtPositionFunction][id=%s] createAndPublishDebtPosition executed in [%s] ms", context.getInvocationId(), (endTime - startTime)));

            if (!failed.isEmpty()) {
                logger.log(Level.WARNING, () -> String.format("[CuCreateDebtPositionFunction] retry failed rows , message: %s", message));
                handleFailedRows(logger, debtPositions, failed, context.getInvocationId());
            }
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction END][id=" + context.getInvocationId() + "] processed a message");
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR][id=" + context.getInvocationId() + "] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - message: " + message);
        }

    }

    /**
     * @param logger        for logging
     * @param debtPositions message
     * @param failed        list of failed rows
     * @param invocationId
     */
    private void handleFailedRows(Logger logger, DebtPositionMessage debtPositions, List<DebtPositionRowMessage> failed, String invocationId) {
        int maxRetry = maxAttempts != null ? Integer.parseInt(maxAttempts) : 0;
        // retry
        if (debtPositions.getRetryCount() < maxRetry) {
            failed.forEach(elem ->
                    logger.log(Level.FINE, () -> String.format("[CuCreateDebtPositionFunction][requestId=%s] Retry iuv: %s", invocationId + ":" + elem.getId(), elem.getIuv()))
            );

            // insert message in queue
            var queueService = getDebtPositionQueueService(logger);
            queueService.insertMessage(DebtPositionMessage.builder()
                    .csvFilename(debtPositions.getCsvFilename())
                    .retryCount(debtPositions.getRetryCount() + 1)
                    .rows(failed)
                    .build());

        } else {
            // update with ERROR status
            failed.forEach(row -> {
                String requestId = invocationId + ":" + row.getId();
                logger.log(Level.WARNING, () -> String.format("[CuCreateDebtPositionFunction][requestId=%s] Update entity with ERROR status", requestId));
                updateTable(debtPositions.getCsvFilename(), logger, row, false, requestId);
            });
        }
    }

    /**
     * calls GPD for the status and updates the elem status in the table
     *
     * @param filename     used as partition key
     * @param logger       for logging
     * @param row          element to process
     * @param invocationId
     */

    private RetryStep createAndPublishDebtPosition(String filename, Logger logger, DebtPositionRowMessage row, String invocationId) {
        String requestId = invocationId + ":" + row.getId();
        logger.log(Level.FINE, () -> "[CuCreateDebtPositionFunction][requestId=" + requestId + "] filename:" + filename + " id:" + row.getId());
        switch (RetryStep.valueOf(row.getRetryAction())) {
            case NONE:
            case CREATE:
                var statusCreate = this.createDebtPosition(logger, row, requestId);
                if (!statusCreate) {
                    row.setRetryAction(RetryStep.CREATE.name());
                    return RetryStep.CREATE;
                }
            case PUBLISH:
                var statusPublish = this.publishDebtPosition(logger, row, requestId);
                if (!statusPublish) {
                    row.setRetryAction(RetryStep.PUBLISH.name());
                    return RetryStep.PUBLISH;
                }
            default:
                // update entity
                logger.log(Level.FINE, () -> "[CuCreateDebtPositionFunction][requestId=" + requestId + "] Updating table: [paIdFiscalCode= " + row.getPaIdFiscalCode() + "; debtorIdFiscalCode=" + row.getDebtorIdFiscalCode() + "]");
                updateTable(filename, logger, row, true, requestId);
                row.setRetryAction(RetryStep.DONE.name());
                return RetryStep.DONE;
        }

    }

    private void updateTable(String filename, Logger logger, DebtPositionRowMessage row, boolean status, String requestId) {
        var tableService = getDebtPositionTableService(logger);
        tableService.updateEntity(filename, row, status, requestId);
    }


    private boolean createDebtPosition(Logger logger, DebtPositionRowMessage row, String requestId) {
        // get status from GPD
        GpdClient gpdClient = this.getGpdClientInstance();
        PaymentPositionModel body = PaymentPositionModel.builder()
                .iupd(row.getIupd())
                .type("G")
                .fiscalCode(row.getDebtorIdFiscalCode())
                .fullName(row.getDebtorName())
                .email(row.getDebtorEmail())
                .companyName(row.getCompanyName())
                .paymentOption(List.of(PaymentOptionModel.builder()
                        .iuv(row.getIuv())
                        .amount(row.getAmount())
                        .description("Canone Unico Patrimoniale - CORPORATE")
                        .isPartialPayment(false)
                        .dueDate("2023-04-30T23:59:59.999Z")
                        .transfer(List.of(Transfer.builder()
                                .idTransfer("1")
                                .amount(row.getAmount())
                                .remittanceInformation("Canone Unico Patrimoniale - CORPORATE")
                                .category("0201138TS")
                                .iban(row.getIban())
                                .build()))
                        .build()))
                .build();

        return gpdClient.createDebtPosition(logger, row.getPaIdFiscalCode(), body, requestId);
    }

    private boolean publishDebtPosition(Logger logger, DebtPositionRowMessage row, String requestId) {
        GpdClient gpdClient = this.getGpdClientInstance();
        return gpdClient.publishDebtPosition(logger, row.getPaIdFiscalCode(), row.getIupd(), requestId);
    }

    protected GpdClient getGpdClientInstance() {
        return GpdClient.getInstance();
    }

    protected DebtPositionTableService getDebtPositionTableService(Logger logger) {
        return new DebtPositionTableService(logger);
    }

    protected DebtPositionQueueService getDebtPositionQueueService(Logger logger) {
        return new DebtPositionQueueService(logger);
    }


}
