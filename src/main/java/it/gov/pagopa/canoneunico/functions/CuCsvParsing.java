package it.gov.pagopa.canoneunico.functions;

import com.azure.core.implementation.serializer.DefaultJsonSerializer;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.systemevents.StorageBlobCreatedEventData;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.opencsv.bean.CsvToBean;
import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.model.BlobInfo;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.service.CuCsvService;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class CuCsvParsing {

    private static final String LOG_VALIDATION_PREFIX = "[CuCsvParsingFunction Error] Validation Error: ";
    private static final String LOG_VALIDATION_ERROR_HEADER = "Error during csv validation {filename = %s; nLinesError/nTotLines = %s}";
    private static final String LOG_VALIDATION_ERROR_DETAIL = "{line = %s } - {errors = %s}";
    private static final String INPUT_DIRECTORY_NAME = "input";
    private static final String ERROR_DIRECTORY_NAME = "error";

    /**
     * This function will be invoked when a new or updated blob is detected at the
     * specified path. The blob contents are provided as input to this function.
     */
    @FunctionName("CuCsvParsingFunction")
    public void run(
            @QueueTrigger(name = "BlobCreatedEventTrigger", queueName = "%CU_BLOB_EVENTS_QUEUE%", connection = "CU_SA_CONNECTION_STRING") String events,
            final ExecutionContext context) {
        Logger logger = context.getLogger();
        LocalDateTime start = LocalDateTime.now();

        try {
            BlobInfo blobInfo = getDataFromEvent(context, events);

            logger.log(Level.INFO, () ->
                    String.format("[CuCsvParsingFunction START] execution started at [%s] - fileName [%s]",
                            start, blobInfo.getName()));

            // get byte content and convert to String type
            BinaryData content = getContent(context, blobInfo);
            String converted = new String(content.toBytes(), StandardCharsets.UTF_8);

            // initialize csvService and info from ecConfig
            CuCsvService csvService = this.getCuCsvServiceInstance(logger);
            csvService.initEcConfigList();
            DebtPositionValidationCsv csvValidation = validateCsv(blobInfo.getName(), logger, csvService, converted);

            String fileName = blobInfo.getName();
            String fileKey = AzuriteStorageUtil.getBlobKey(blobInfo.getContainer(), fileName);
            if (csvValidation.getErrorRows().isEmpty()) {
                // If valid file -> save on table and write on queue
                handleValidFile(fileKey, logger, start, csvService, csvValidation);
            } else {
                // If not valid file -> write log error, save on 'error' blob space and delete from 'input' blob space
                handleInvalidFile(blobInfo, logger, start, csvService, converted, csvValidation);
            }

            Runtime.getRuntime().gc();
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> String.format(
                    LOG_VALIDATION_PREFIX + "[CuCsvParsingFunction ERROR] [%s] Generic Error: error msg = %s - cause = %s", context.getInvocationId(), e.getMessage(), e.getCause()));
        }
    }

    // return downloaded blob
    public BinaryData getContent(ExecutionContext context, BlobInfo blobInfo) throws CanoneUnicoException {
        BinaryData content = new AzuriteStorageUtil().downloadBlob(context, blobInfo.getContainer(), blobInfo.getDirectory() + '/' + blobInfo.getName());
        if(content == null)
            throw new CanoneUnicoException(String.format("[CuCsvParsing] Blob not found, corporate: %s, file: %s", blobInfo.getContainer(), blobInfo.getName()));
        return content;
    }

    // return data: [container-name, filename]
    public BlobInfo getDataFromEvent(ExecutionContext context, String events) throws CanoneUnicoException {
        Logger logger = context.getLogger();
        List<EventGridEvent> eventGridEvents = EventGridEvent.fromString(events);

        if (eventGridEvents.isEmpty()) {
            throw new CanoneUnicoException("[CuCsvParsing] Empty event list.");
        }
        EventGridEvent event = eventGridEvents.get(0);
        if (!event.getEventType().equals("Microsoft.Storage.BlobCreated")) {
            throw new CanoneUnicoException("[CuCsvParsing] Event not equals to Microsoft.Storage.BlobCreated.");
        }

        logger.log(Level.INFO, () -> String.format("[id=%s][CuCsvParsing] Call event type %s handler.", context.getInvocationId(), event.getEventType()));
        StorageBlobCreatedEventData blobData = event.getData().toObject(StorageBlobCreatedEventData.class, new DefaultJsonSerializer());

        if (blobData.getContentLength() > 1e+8 || blobData.getContentLength() == 0) { // if file greater than 100 MB or 0 MB
            throw new CanoneUnicoException("[CuCsvParsing] File length not allowed: " + blobData.getContentLength() + " MB");
        }

        logger.log(Level.INFO, () -> String.format("[id=%s][CuCsvParsing] Blob event subject: %s", context.getInvocationId(), event.getSubject()));
        Pattern pattern = Pattern.compile("containers/(\\w+)/blobs/"+INPUT_DIRECTORY_NAME+"/([\\w-/]+\\.csv)");
        Matcher matcher = pattern.matcher(event.getSubject());

        // Check if the pattern is found
        if (matcher.find()) {
            return BlobInfo.builder()
                           .container(matcher.group(1))
                           .directory(INPUT_DIRECTORY_NAME)
                           .name(matcher.group(2)).build();
        } else {
            throw new CanoneUnicoException("[CuCsvParsing] Wrong match in subject: " + event.getSubject());
        }
    }

    private DebtPositionValidationCsv validateCsv(String fileName, Logger logger, CuCsvService csvService, String converted) {
        // parse CSV file to create an object based on 'PaymentNotice' bean
        long startTime = System.currentTimeMillis();
        CsvToBean<PaymentNotice> csvToBean = csvService.parseCsvToBean(converted);
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: parseCsv executed", fileName));

        // Check if CSV is valid
        DebtPositionValidationCsv csvValidation = CsvValidation.checkCsvIsValid(logger, fileName, csvToBean);
        long endTime = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: parseCsv and checkCsvIsValid executed in [%s] ms", fileName, (endTime - startTime)));
        return csvValidation;
    }

    private void handleInvalidFile(BlobInfo blobInfo, Logger logger, LocalDateTime start, CuCsvService csvService, String converted, DebtPositionValidationCsv csvValidation) {
        String filename = blobInfo.getName();
        // log
        String header = LOG_VALIDATION_PREFIX + String.format(LOG_VALIDATION_ERROR_HEADER,
                filename,
                csvValidation.getNumberInvalidRows() + "/" + csvValidation.getTotalNumberRows());
        List<String> details = new ArrayList<>();
        csvValidation.getErrorRows().forEach(exception ->
                details.add(String.format(LOG_VALIDATION_ERROR_DETAIL, exception.getRowNumber() - 1, exception.getErrorsDetail()))
        );
        logger.log(Level.SEVERE, () -> header + System.lineSeparator() + details);

        // Create error file
        long startTime1 = System.currentTimeMillis();
        String errorCSV = csvService.generateRowsErrorCsv(converted, csvValidation);
        long endTime1 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] generateErrorCsv executed in [%s] ms", filename, (endTime1 - startTime1)));

        // Upload file in error blob storage
        long startTime2 = System.currentTimeMillis();
        csvService.uploadCsv(blobInfo.getContainer(), ERROR_DIRECTORY_NAME + '/' + filename, errorCSV);
        long endTime2 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] uploadCsv executed in [%s] ms", filename, (endTime2 - startTime2)));

        // Delete the original file from input blob storage
        long startTime3 = System.currentTimeMillis();
        csvService.deleteCsv(blobInfo.getContainer(), blobInfo.getDirectory() + '/' + filename);
        long endTime3 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] deleteCsv executed in [%s] ms", filename, (endTime3 - startTime3)));


        logger.log(Level.INFO, () -> String.format(
                "[CuCsvParsingFunction END] [%s] execution started at [%s] and ended at [%s]",
                filename, start, LocalDateTime.now()));
    }

    private void handleValidFile(String fileKey, Logger logger, LocalDateTime start, CuCsvService csvService, DebtPositionValidationCsv csvValidation) throws CanoneUnicoException {
        // convert `CsvToBean` object to list of payments
        final List<PaymentNotice> payments = csvValidation.getPayments();
        // save in Table
        long startTime1 = System.currentTimeMillis();
        List<DebtPositionEntity> savedEntities = csvService.saveDebtPosition(fileKey, payments);
        long endTime1 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: saveDebtPositionTable executed in [%s] ms", fileKey, (endTime1 - startTime1)));

        // push in queue
        long startTime2 = System.currentTimeMillis();
        csvService.pushDebtPosition(fileKey, savedEntities);
        long endTime2 = System.currentTimeMillis();
        logger.log(Level.INFO, () -> String.format("[CuCsvParsingFunction] [%s] time: saveDebtPositionQueue executed in [%s] ms", fileKey, (endTime2 - startTime2)));

        logger.log(Level.INFO, () -> String.format(
                "[CuCsvParsingFunction END] [%s] execution started at [%s] and ended at [%s]",
                fileKey, start, LocalDateTime.now()));
    }

    public CuCsvService getCuCsvServiceInstance(Logger logger) {
        return new CuCsvService(logger);
    }
}
