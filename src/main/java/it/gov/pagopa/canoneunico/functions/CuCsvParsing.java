package it.gov.pagopa.canoneunico.functions;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.opencsv.CSVWriter;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.model.PaymentNoticeError;
import it.gov.pagopa.canoneunico.csv.model.service.CuCsvService;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.csv.validaton.PaymentNoticeVerifier;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;

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
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String containerInputBlob = System.getenv("INPUT_CSV_BLOB");
    private String containerErrorBlob = System.getenv("ERROR_CSV_BLOB");
    

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

    	logger.log(Level.INFO, () -> "Blob Trigger function executed at: " + LocalDateTime.now() + " for blob " + fileName);
    	
    	CuCsvService csvService = this.getCuCsvServiceInstance(logger);

    	// CSV File
    	String converted = new String(content, StandardCharsets.UTF_8);
    	logger.log(Level.INFO, () -> converted);

    	// parse CSV file to create a list of 'PaymentNotice' objects
    	try (Reader reader = new StringReader(converted)) {
    		
    		// Create Mapping Strategy to arrange the column name
            HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy=
                        new HeaderColumnNameMappingStrategy<>();
            mappingStrategy.setType(PaymentNotice.class);
    		
    		CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
    				.withSeparator(';')
    				.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
    				.withOrderedResults(true)
    				.withMappingStrategy(mappingStrategy)
    				.withVerifier(new PaymentNoticeVerifier())
    				.withType(PaymentNotice.class)
    				.withIgnoreLeadingWhiteSpace(true)
    				.withThrowExceptions(false)
    				.build();
    		
    		// convert `CsvToBean` object to list of users
    		final List<PaymentNotice> payments = csvToBean.parse();
    		
    		// CSV validation 
    		DebtPositionValidationCsvError csvValidationErrors = CsvValidation.checkCsvIsValid(fileName, csvToBean, payments);
    		if (!csvValidationErrors.getErrorRows().isEmpty()) {
    			String header = LOG_VALIDATION_PREFIX + String.format(LOG_VALIDATION_ERROR_HEADER, 
    					fileName, 
    					csvValidationErrors.getNumberInvalidRows()+"/"+csvValidationErrors.getTotalNumberRows());
    			List<String> details = new ArrayList<>();
    			csvValidationErrors.getErrorRows().stream().forEach(exception -> { 
    				details.add(String.format(LOG_VALIDATION_ERROR_DETAIL, exception.getRowNumber(), exception.getErrorsDetail()));
        		});
    			logger.log(Level.SEVERE, () -> header + System.lineSeparator() + details);
    			
    			StringWriter errorCSV = this.generateErrorCsvFile(converted, csvValidationErrors);
    			// Spostare il file nella cartella error del blob storage
    			csvService.create(containerErrorBlob, fileName, errorCSV.toString());
    			// Rimuovere il file originale dalla cartella di input
    			csvService.delete(containerInputBlob, fileName);
    		}
    		
    		
    		

    		// TODO: save in Table
    		


    	} catch (Exception e) {

    		logger.log(Level.SEVERE, () -> "[CuCsvParsingFunction Error] Generic Error " + e.getMessage() + " "
    				+ e.getCause());
    	}
    	
    	
    }
    
    public CuCsvService getCuCsvServiceInstance(Logger logger) {
        return new CuCsvService(this.storageConnectionString, logger);
    }
    
    private StringWriter generateErrorCsvFile (String converted, DebtPositionValidationCsvError csvValidationErrors) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException, IOException {
    	Reader reader = new StringReader(converted);
    	
    	// Create Mapping Strategy to arrange the column name
        HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy=
                    new HeaderColumnNameMappingStrategy<>();
        mappingStrategy.setType(PaymentNotice.class);
    	
    	CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
				.withSeparator(';')
				.withOrderedResults(true)
				.withMappingStrategy(mappingStrategy)
				.withType(PaymentNotice.class)
				.build();
	    
	    List<PaymentNoticeError> paymentsToWrite = ObjectMapperUtils.mapAll(csvToBean.parse(), PaymentNoticeError.class) ; 
	    
	    for (DebtPositionErrorRow e : csvValidationErrors.getErrorRows()) {
	    	PaymentNoticeError p = paymentsToWrite.get((int) (e.getRowNumber() - 2));
	    	p.setErrorsNote("validation error: " + e.getErrorsDetail());
	    }
    	
	    
	    // Create Mapping Strategy to arrange the column name
	    ColumnPositionMappingStrategy<PaymentNoticeError> mappingStrategyWrite=
                    new ColumnPositionMappingStrategy<>();
        mappingStrategyWrite.setType(PaymentNoticeError.class);
        
	    StringWriter writer = new StringWriter();
	    StatefulBeanToCsvBuilder<PaymentNoticeError> builder = new StatefulBeanToCsvBuilder<>(writer);
	    StatefulBeanToCsv<PaymentNoticeError> beanWriter = builder
	    		  .withSeparator(';')
	    		  .withMappingStrategy(mappingStrategyWrite)
	    		  .withOrderedResults(true)
	              .build();
	    beanWriter.write(paymentsToWrite);
	    
	    String headers = "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_mail;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;"
	    		+ "debtor_name;debtor_email;payment_notice_number;note;errors_note";
        StringWriter csv = new StringWriter();
        csv.append(headers);
        csv.append(System.lineSeparator());
	    csv.append(writer.toString());
	    csv.flush();
	    csv.close();
	    return csv;
    }
        

}
