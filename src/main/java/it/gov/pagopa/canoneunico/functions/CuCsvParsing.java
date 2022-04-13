package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.opencsv.bean.CsvToBean;
import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.service.CuCsvService;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class CuCsvParsing {

    private static final String LOG_VALIDATION_PREFIX = "[CuCsvParsingFunction Error] Validation Error: ";
    private static final String LOG_VALIDATION_ERROR_HEADER = "Error during csv validation {filename = %s; nLinesError/nTotLines = %s}";
    private static final String LOG_VALIDATION_ERROR_DETAIL = "{line = %s } - {errors = %s}";

    /**
     * This function will be invoked when a new or updated blob is detected at the
     * specified path. The blob contents are provided as input to this function.
     */
    @FunctionName("CuCsvParsingFunction")
    public void run(
            @BlobTrigger(name = "BlobCsvTrigger", path = "%INPUT_CSV_BLOB%/{name}", dataType = "binary", connection = "CU_SA_CONNECTION_STRING") byte[] content,
            @BindingName("name") String fileName, final ExecutionContext context) {

        Logger logger = context.getLogger();
        try {
            LocalDateTime start = LocalDateTime.now();
            logger.log(Level.INFO, () ->
                    String.format(
                            "[CuCsvParsingFunction START] execution started at [%s] - fileName [%s]",
                            start, fileName));

            CuCsvService csvService = this.getCuCsvServiceInstance(logger);

            // initialize info from ecConfig
            csvService.initEcConfigList();

            // string CSV File
            String converted = new String(content, StandardCharsets.UTF_8);

            DebtPositionValidationCsv csvValidation = validateCsv(fileName, logger, csvService, converted);

            if (csvValidation.getErrorRows().isEmpty()) {
                // If valid file -> save on table and write on queue
                handleValidFile(fileName, logger, start, csvService, csvValidation);
            } else {
                // If not valid file -> write log error, save on 'error' blob space and delete from 'input' blob space
                handleInvalidFile(fileName, logger, start, csvService, converted, csvValidation);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> String.format(
                    LOG_VALIDATION_PREFIX + "[CuCsvParsingFunction ERROR] [%s] Generic Error: error msg = %s - cause = %s",
                    fileName, e.getMessage(), e.getCause()));
        }

    }

    private DebtPositionValidationCsv validateCsv(String fileName, Logger logger, CuCsvService csvService, String converted) {
        // parse CSV file to create an object based on 'PaymentNotice' bean
        long startTime = System.currentTimeMillis();
        CsvToBean<PaymentNotice> csvToBean = csvService.parseCsv(converted);

        // Check if CSV is valid
        DebtPositionValidationCsv csvValidation = CsvValidation.checkCsvIsValid(fileName, csvToBean);
        long endTime = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: parseCsv and checkCsvIsValid executed in [%s] ms", fileName, (endTime - startTime)));
        return csvValidation;
    }

    private void handleInvalidFile(String fileName, Logger logger, LocalDateTime start, CuCsvService csvService, String converted, DebtPositionValidationCsv csvValidation) {
        // log
        String header = LOG_VALIDATION_PREFIX + String.format(LOG_VALIDATION_ERROR_HEADER,
                fileName,
                csvValidation.getNumberInvalidRows() + "/" + csvValidation.getTotalNumberRows());
        List<String> details = new ArrayList<>();
        csvValidation.getErrorRows().forEach(exception ->
                details.add(String.format(LOG_VALIDATION_ERROR_DETAIL, exception.getRowNumber() - 1, exception.getErrorsDetail()))
        );
        logger.log(Level.SEVERE, () -> header + System.lineSeparator() + details);

        // Create error file
        long startTime1 = System.currentTimeMillis();
        String errorCSV = csvService.generateErrorCsv(converted, csvValidation);
        long endTime1 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] generateErrorCsv executed in [%s] ms", fileName, (endTime1 - startTime1)));

        // Upload file in error blob storage
        long startTime2 = System.currentTimeMillis();
        csvService.uploadCsv(fileName, errorCSV);
        long endTime2 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] uploadCsv executed in [%s] ms", fileName, (endTime2 - startTime2)));

        // Delete the original file from input blob storage
        long startTime3 = System.currentTimeMillis();
        csvService.deleteCsv(fileName);
        long endTime3 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] deleteCsv executed in [%s] ms", fileName, (endTime3 - startTime3)));


        logger.log(Level.INFO, () -> String.format(
                "[CuCsvParsingFunction END] [%s] execution started at [%s] and ended at [%s]",
                fileName, start, LocalDateTime.now()));
    }

    private void handleValidFile(String fileName, Logger logger, LocalDateTime start, CuCsvService csvService, DebtPositionValidationCsv csvValidation) throws CanoneUnicoException {
        // convert `CsvToBean` object to list of payments
        final List<PaymentNotice> payments = csvValidation.getPayments();
        // save in Table
        long startTime1 = System.currentTimeMillis();
        List<DebtPositionEntity> savedEntities = csvService.saveDebtPosition(fileName, payments);
        long endTime1 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: saveDebtPositionTable executed in [%s] ms", fileName, (endTime1 - startTime1)));

        // push in queue
        long startTime2 = System.currentTimeMillis();
        csvService.pushDebtPosition(fileName, savedEntities);
        long endTime2 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: saveDebtPositionQueue executed in [%s] ms", fileName, (endTime2 - startTime2)));

        logger.log(Level.INFO, () -> String.format(
                "[CuCsvParsingFunction END] [%s] execution started at [%s] and ended at [%s]",
                fileName, start, LocalDateTime.now()));
    }

    public CuCsvService getCuCsvServiceInstance(Logger logger) {
        return new CuCsvService(logger);
    }
}
