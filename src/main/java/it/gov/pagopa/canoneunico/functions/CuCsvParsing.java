package it.gov.pagopa.canoneunico.functions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class CuCsvParsing {
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");

    /**
     * This function will be invoked when a new or updated blob is detected at the
     * specified path. The blob contents are provided as input to this function.
     *
     */
    @FunctionName("CuCsvParsingFunction")
    public void run(
    		@BlobTrigger(name = "BlobCsvTrigger", path = "%INPUT_CSV_BLOB%/{name}", dataType = "binary", connection = "CU_SA_CONNECTION_STRING") byte[] content,
    		@BindingName("name") String fileName, final ExecutionContext context) {

    	// CSV_BLOB = input
    	Logger logger = context.getLogger();

    	logger.log(Level.INFO, () -> "Blob Trigger function executed at: " + LocalDateTime.now() + " for blob " + fileName);

    	// CSV File
    	String converted = new String(content, StandardCharsets.UTF_8);
    	logger.log(Level.INFO, () -> converted);

    	// parse CSV file to create a list of 'PaymentNotice' objects
    	try (Reader reader = new StringReader(converted)) {
    		

    		// create csv bean reader
    		CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
    				.withSeparator(';')
    				.withType(PaymentNotice.class)
    				.withIgnoreLeadingWhiteSpace(true)
    				.withThrowExceptions(false)
    				.build();

    		// convert `CsvToBean` object to list of users
    		final List<PaymentNotice> payments = csvToBean.parse();
    		
    		// CSV validation 
    		DebtPositionValidationCsvError csvValidationErrors = CsvValidation.checkCsvIsValid(fileName, csvToBean, payments);
    		csvValidationErrors.getErrorRows().stream().forEach((exception) -> { 
    			logger.log(Level.SEVERE, "******** Inconsistent data:" + exception.getRowNumber() +"; "+exception.getErrorsDetail());
    		});
    		
    		/*
    		payments.stream().forEach((p) -> {
    	        logger.info("Parsed data:" + p.toString());
    	    });
    		
    		csvToBean.getCapturedExceptions().stream().forEach((exception) -> { 
    	        logger.log(Level.SEVERE, "******** Inconsistent data:" + 
    	                      String.join("", exception.getLine()), exception);
    	    });*/

    		

    		// TODO: save users in Table
    		


    	} catch (Exception e) {

    		logger.log(Level.SEVERE, () -> "[CuCsvParsingFunction Error] Generic Error " + e.getMessage() + " "
    				+ e.getCause());
    	}
    	
    	
    }
        

}
