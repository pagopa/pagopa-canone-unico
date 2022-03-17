package it.gov.pagopa.canoneunico.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.model.PaymentOptionModel;
import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import it.gov.pagopa.canoneunico.model.Transfer;
import it.gov.pagopa.canoneunico.service.DebtPositionTableService;
import it.gov.pagopa.canoneunico.service.GpdClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with Azure Queue trigger.
 */
public class CuCreateDebtPosition {

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
            debtPositions.getRows()
                    .parallelStream()
                    .forEach(row -> createDebtPosition(debtPositions.getCsvFilename(), logger, row));

            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction END]  processed a message " + message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - message " + message);
        }

    }

    /**
     * calls GPD for the status and updates the elem status in the table
     *
     * @param filename used as partition key
     * @param logger   for logging
     * @param row      element to process
     */
    private void createDebtPosition(String filename, Logger logger, DebtPositionRowMessage row) {
        // get status from GPD

        GpdClient gpdClient = this.getGpdClientInstance();

        var status = gpdClient.createDebtPosition(logger, row.getFiscalCode(), PaymentPositionModel.builder()
                .iupd(row.getIupd())
                .type("G")
                .fiscalCode(row.getFiscalCode())
                .fullName(row.getDebtorName())
                .email(row.getDebtorEmail())
                .companyName(row.getCompanyName())
                .validityDate(LocalDateTime.now().plusDays(1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
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
                .build());

        // update entity
        logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction] Updating table: " + row.getFiscalCode());
        var tabelService = getDebtPositionService(logger);
        tabelService.updateEntity(filename, row, status);

    }

    protected GpdClient getGpdClientInstance() {
        return GpdClient.getInstance();
    }

    protected DebtPositionTableService getDebtPositionService(Logger logger) {
        return new DebtPositionTableService(logger);
    }


}
