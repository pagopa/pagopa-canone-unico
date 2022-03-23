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
        logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction START] new message " + message);


        try {
            // map message in a model
            var debtPositions = new ObjectMapper().readValue(message, DebtPositionMessage.class);

            // in parallel, for each element in the message calls GPD for the status and updates the elem status in the table
            var failed = debtPositions.getRows()
                    .parallelStream()
                    .filter(row -> !RetryStep.DONE.equals(createAndPublishDebtPosition(debtPositions.getCsvFilename(), logger, row)))
                    .collect(Collectors.toList());

            if (!failed.isEmpty()) {
                handleFailedRows(logger, debtPositions, failed);
            }
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction END]  processed a message " + message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - message " + message);
        }

    }

    /**
     * @param logger        for logging
     * @param debtPositions message
     * @param failed        list of failed rows
     */
    private void handleFailedRows(Logger logger, DebtPositionMessage debtPositions, List<DebtPositionRowMessage> failed) {
        int maxRetry = maxAttempts != null ? Integer.parseInt(maxAttempts) : 0;
        // retry
        if (debtPositions.getRetryCount() < maxRetry) {
            // insert message in queue
            var queueService = getDebtPositionQueueService(logger);
            queueService.insertMessage(DebtPositionMessage.builder()
                    .csvFilename(debtPositions.getCsvFilename())
                    .retryCount(debtPositions.getRetryCount() + 1)
                    .rows(failed)
                    .build());

        } else {
            // update with ERROR status
            failed.forEach(row -> updateTable(debtPositions.getCsvFilename(), logger, row, false));
        }
    }

    /**
     * calls GPD for the status and updates the elem status in the table
     *
     * @param filename used as partition key
     * @param logger   for logging
     * @param row      element to process
     */

    private RetryStep createAndPublishDebtPosition(String filename, Logger logger, DebtPositionRowMessage row) {

        switch (RetryStep.valueOf(row.getRetryAction())) {
            case NONE:
            case CREATE:
                var statusCreate = this.createDebtPosition(logger, row);
                if (!statusCreate) {
                    row.setRetryAction(RetryStep.CREATE.name());
                    return RetryStep.CREATE;
                }
            case PUBLISH:
                var statusPublish = this.publishDebtPosition(logger, row);
                if (!statusPublish) {
                    row.setRetryAction(RetryStep.PUBLISH.name());
                    return RetryStep.PUBLISH;
                }
            default:
                // update entity
                logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction] Updating table: [paIdFiscalCode= " + row.getPaIdFiscalCode() + "; debtorIdFiscalCode=" + row.getDebtorIdFiscalCode() + "]");
                updateTable(filename, logger, row, true);
                row.setRetryAction(RetryStep.DONE.name());
                return RetryStep.DONE;
        }

    }

    private void updateTable(String filename, Logger logger, DebtPositionRowMessage row, boolean status) {
        var tableService = getDebtPositionTableService(logger);
        tableService.updateEntity(filename, row, status);
    }


    private boolean createDebtPosition(Logger logger, DebtPositionRowMessage row) {
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

        return gpdClient.createDebtPosition(logger, row.getPaIdFiscalCode(), body);
    }

    private boolean publishDebtPosition(Logger logger, DebtPositionRowMessage row) {
        GpdClient gpdClient = this.getGpdClientInstance();
        return gpdClient.publishDebtPosition(logger, row.getPaIdFiscalCode(), row.getIupd());
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
