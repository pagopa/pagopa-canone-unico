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
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction START][id=" + context.getInvocationId() + "][" + debtPositions.getCsvFilename() + "] create debt position of the file");


            // in parallel, for each element in the message calls GPD for the status and updates the elem status in the table
            long startTime = System.currentTimeMillis();

            var failed = debtPositions.getRows()
                    .stream()
                    .filter(row -> {
                        RetryStep retryStep = createAndPublishDebtPosition(debtPositions.getCsvFilename(), logger, row, context.getInvocationId());
                        row.setRetryAction(retryStep.name());
                        return !RetryStep.DONE.equals(retryStep);
                    })
                    .collect(Collectors.toList());

            long endTime = System.currentTimeMillis();
            logger.log(Level.FINE, () -> String.format("[CuCreateDebtPositionFunction][id=%s][%s] createAndPublishDebtPosition executed in [%s] ms", context.getInvocationId(), debtPositions.getCsvFilename(), (endTime - startTime)));

            if (!failed.isEmpty()) {
                logger.log(Level.WARNING, () -> String.format("[CuCreateDebtPositionFunction][id=%s][%s] retry failed rows , message: %s", context.getInvocationId(), debtPositions.getCsvFilename(), message));
                handleFailedRows(logger, debtPositions, failed, context.getInvocationId());
            }
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction END][id=" + context.getInvocationId() + "][" + debtPositions.getCsvFilename() + "] processed a message");
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR][id=" + context.getInvocationId() + "] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - message: " + message);
        }

    }

    /**
     * @param logger        for logging
     * @param debtPositions message
     * @param failed        list of failed rows
     * @param invocationId  invocation id for logging
     */
    private void handleFailedRows(Logger logger, DebtPositionMessage debtPositions, List<DebtPositionRowMessage> failed, String invocationId) {
        int maxRetry = maxAttempts != null ? Integer.parseInt(maxAttempts) : 0;

        // if elem is failed with 4xx HTTP status code, it isn't retryable
        var notRetryable = failed.stream()
                .filter(elem -> RetryStep.ERROR.name().equals(elem.getRetryAction()))
                .collect(Collectors.toList());

        var retryable = failed.stream()
                .filter(elem -> !RetryStep.ERROR.name().equals(elem.getRetryAction()))
                .collect(Collectors.toList());

        // retry only if maxRetry is not reached
        if (!retryable.isEmpty() && debtPositions.getRetryCount() < maxRetry) {
            retryable
                    .forEach(elem ->
                            logger.log(Level.FINE, () -> String.format("[CuCreateDebtPositionFunction][requestId=%s][%s] Retry iuv: %s", invocationId + ":" + elem.getId(), debtPositions.getCsvFilename(), elem.getIuv()))
                    );

            // insert message in queue
            var queueService = getDebtPositionQueueService(logger);
            queueService.insertMessage(DebtPositionMessage.builder()
                    .csvFilename(debtPositions.getCsvFilename())
                    .retryCount(debtPositions.getRetryCount() + 1)
                    .rows(retryable)
                    .build());

        } else {
            // stop retry at max attempts
            notRetryable.addAll(retryable);
        }
        if (!notRetryable.isEmpty()) {
            // update with ERROR status
            notRetryable.forEach(row -> {
                String requestId = invocationId + ":" + row.getId();
                logger.log(Level.WARNING, () -> String.format("[CuCreateDebtPositionFunction][requestId=%s][%s] Update entity with ERROR status", requestId, debtPositions.getCsvFilename()));
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
     * @param invocationId invocation id for logging
     */

    private RetryStep createAndPublishDebtPosition(String filename, Logger logger, DebtPositionRowMessage row, String invocationId) {
        String requestId = invocationId + ":" + row.getId();
        logger.log(Level.FINE, () -> "[CuCreateDebtPositionFunction][requestId=" + requestId + "][" + filename + "] row id:" + row.getId());
        switch (RetryStep.valueOf(row.getRetryAction())) {
            case NONE:
            case CREATE:
                var statusCreate = this.createDebtPosition(logger, row, requestId);
                if (statusCreate >= 400 && statusCreate < 500) {
                    return RetryStep.ERROR;
                }
                if (statusCreate != 201) {
                    row.setRetryAction(RetryStep.CREATE.name());
                    return RetryStep.CREATE;
                }
            case PUBLISH:
                var statusPublish = this.publishDebtPosition(logger, row, requestId);
                if (statusPublish >= 400 && statusPublish < 500) {
                    return RetryStep.ERROR;
                }
                if (statusPublish != 200) {
                    row.setRetryAction(RetryStep.PUBLISH.name());
                    return RetryStep.PUBLISH;
                }
            default:
                // update entity
                logger.log(Level.FINE, () -> "[CuCreateDebtPositionFunction][requestId=" + requestId + "][" + filename + "] Updating table: [paIdFiscalCode= " + row.getPaIdFiscalCode() + "; debtorIdFiscalCode=" + row.getDebtorIdFiscalCode() + "]");
                updateTable(filename, logger, row, true, requestId);
                logger.log(Level.FINE, () -> "[CuCreateDebtPositionFunction][requestId=" + requestId + "][" + filename + "] Updated table with CREATED status: [paIdFiscalCode= " + row.getPaIdFiscalCode() + "; debtorIdFiscalCode=" + row.getDebtorIdFiscalCode() + "]");
                return RetryStep.DONE;
        }

    }

    private void updateTable(String filename, Logger logger, DebtPositionRowMessage row, boolean status, String requestId) {
        var tableService = getDebtPositionTableService(logger);
        tableService.updateEntity(filename, row, status, requestId);
    }


    private int createDebtPosition(Logger logger, DebtPositionRowMessage row, String requestId) {
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
                                .category("0101108TS")
                                .iban(row.getIban())
                                .build()))
                        .build()))
                .build();

        return gpdClient.createDebtPosition(logger, row.getPaIdFiscalCode(), body, requestId);
    }

    private int publishDebtPosition(Logger logger, DebtPositionRowMessage row, String requestId) {
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
