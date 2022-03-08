package it.gov.pagopa.canoneunico.functions;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.storage.StorageException;
import com.opencsv.bean.CsvToBean;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.service.CuCsvService;
import lombok.Getter;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class CuCsvParsing {
	
	static {
		//TODO: Caricare in memoria ecConfig
	}
	
	private static final String LOG_VALIDATION_PREFIX       = "[CuCsvParsingFunction Error] Validation Error: ";
	private static final String LOG_VALIDATION_ERROR_HEADER = "Error during csv validation {filename = %s; nLinesError/nTotLines = %s}";
	private static final String LOG_VALIDATION_ERROR_DETAIL = "{line = %s } - {errors = %s}";
    
    /**
     * This function will be invoked when a new or updated blob is detected at the
     * specified path. The blob contents are provided as input to this function.
     *
     */
    @FunctionName("CuCsvParsingFunction")
    public void run(
    		@BlobTrigger(name = "BlobCsvTrigger", path = "%INPUT_CSV_BLOB%/{name}", dataType = "binary", connection = "CU_SA_CONNECTION_STRING") byte[] content,
    		@BindingName("name") String fileName, final ExecutionContext context) {

    	Logger logger = context.getLogger();
    	logger.log(Level.INFO, () -> "[CuCsvParsingFunction START] executed at: " + LocalDateTime.now() + " - fileName " + fileName);

    	CuCsvService csvService = this.getCuCsvServiceInstance(logger);

    	// CSV File
    	String converted = new String(content, StandardCharsets.UTF_8);
    	logger.log(Level.INFO, () -> converted);

    	// parse CSV file to create an object based on 'PaymentNotice' bean
    	CsvToBean<PaymentNotice> csvToBean = csvService.parseCsv(converted);

    	// Check if CSV is valid 
    	DebtPositionValidationCsvError csvValidationErrors = CsvValidation.checkCsvIsValid(fileName, csvToBean);
    	if (!csvValidationErrors.getErrorRows().isEmpty()) {
    		// Create log info
    		String header = LOG_VALIDATION_PREFIX + String.format(LOG_VALIDATION_ERROR_HEADER, 
    				fileName, 
    				csvValidationErrors.getNumberInvalidRows()+"/"+csvValidationErrors.getTotalNumberRows());
    		List<String> details = new ArrayList<>();
    		csvValidationErrors.getErrorRows().stream().forEach(exception -> { 
    			details.add(String.format(LOG_VALIDATION_ERROR_DETAIL, exception.getRowNumber()-1, exception.getErrorsDetail()));
    		});
    		logger.log(Level.SEVERE, () -> header + System.lineSeparator() + details);

    		String errorCSV = csvService.generateErrorCsv(converted, csvValidationErrors);
    		// Create file in error blob storage
    		csvService.uploadCsv(fileName, errorCSV);
    		// Delete the original file from input blob storage
    		csvService.deleteCsv(fileName);
    	}

    	try {
    		// convert `CsvToBean` object to list of payments
        	final List<PaymentNotice> payments = csvToBean.parse();
        	// save in Table
			csvService.saveDebtPosition(fileName, payments);
		} catch (InvalidKeyException | URISyntaxException | StorageException e) {
			logger.log(Level.SEVERE, () -> "[CuCsvParsingFunction Error] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - fileName " + fileName);
		}
    	
    	logger.log(Level.INFO, () -> "[CuCsvParsingFunction END] ended at: " + LocalDateTime.now() + " - fileName " + fileName);
		
    }
    
    public CuCsvService getCuCsvServiceInstance(Logger logger) {
        return new CuCsvService(logger);
    }
}
