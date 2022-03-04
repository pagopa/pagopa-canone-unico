package it.gov.pagopa.canoneunico.csv.model.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
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
import it.gov.pagopa.canoneunico.csv.validaton.PaymentNoticeVerifier;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;



public class CuCsvService {

    private String storageConnectionString;
    private Logger logger;

    public CuCsvService(String storageConnectionString, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.logger = logger;
    }
    
    public CsvToBean<PaymentNotice> parseCsv(String content) {

    	Reader reader = new StringReader(content);
    	// Create Mapping Strategy to arrange the column name
    	HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy=
    			new HeaderColumnNameMappingStrategy<>();
    	mappingStrategy.setType(PaymentNotice.class);

    	return new CsvToBeanBuilder<PaymentNotice>(reader)
    			.withSeparator(';')
    			.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
    			.withOrderedResults(true)
    			.withMappingStrategy(mappingStrategy)
    			.withVerifier(new PaymentNoticeVerifier())
    			.withType(PaymentNotice.class)
    			.withIgnoreLeadingWhiteSpace(true)
    			.withThrowExceptions(false)
    			.build();
    }
    
    public void uploadCsv(String containerBlob, String fileName, String content) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerBlob);
		BlockBlobClient blockBlobClient = cont.getBlobClient(fileName).getBlockBlobClient();
		InputStream stream = new ByteArrayInputStream(content.getBytes());
		blockBlobClient.upload(stream, content.getBytes().length);
    }

    public void deleteCsv(String containerBlob, String fileName) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerBlob);
		cont.getBlobClient(fileName).delete();
    }
    
    public String generateErrorCsv (String converted, DebtPositionValidationCsvError csvValidationErrors)  {
    	
    	StringWriter csv = new StringWriter();
    	
    	try(Reader reader = new StringReader(converted)){
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
    	    String footer ="numLinesError/numTotLines;" + csvValidationErrors.getNumberInvalidRows()+"/"+csvValidationErrors.getTotalNumberRows();
            csv.append(headers);
            csv.append(System.lineSeparator());
    	    csv.append(writer.toString());
    	    csv.append(System.lineSeparator());
    	    csv.append(footer);
    	    csv.flush();
    	    csv.close();
    	    
    	} catch (IOException | CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
    		logger.log(Level.SEVERE, () -> "[CuCsvService generateErrorCsv] Error " + e.getMessage() + " "
                    + e.getCause());
    	}
	    return csv.toString();
    }
    
}
