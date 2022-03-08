package it.gov.pagopa.canoneunico.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
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
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;



public class CuCsvService {

	private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String containerInputBlob = System.getenv("INPUT_CSV_BLOB");
    private String containerErrorBlob = System.getenv("ERROR_CSV_BLOB");
	private String debtPositionTable = System.getenv("DEBT_POSITIONS_TABLE");
	private String debtPositionQueue = System.getenv("DEBT_POSITIONS_QUEUE");
	
    private Logger logger;
    

    public CuCsvService(Logger logger) {
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
    
    public void uploadCsv(String fileName, String content) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerErrorBlob);
		BlockBlobClient blockBlobClient = cont.getBlobClient(fileName).getBlockBlobClient();
		InputStream stream = new ByteArrayInputStream(content.getBytes());
		blockBlobClient.upload(stream, content.getBytes().length);
    }

    public void deleteCsv(String fileName) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerInputBlob);
		cont.getBlobClient(fileName).delete();
    }
    
    public void saveDebtPosition(String fileName, List<PaymentNotice> payments) throws InvalidKeyException, URISyntaxException, StorageException {
    	 CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                 .createCloudTableClient()
                 .getTableReference(this.debtPositionTable);
    	 
    	 TableBatchOperation batchOperation = new TableBatchOperation();
    	 
    	 this.getDebtPositionEntities(fileName, payments).forEach(batchOperation::insert);
    	 
    	 table.execute(batchOperation);
    	 
    }
    
    public void pushDebtPosition(String fileName, List<PaymentNotice> payments) throws InvalidKeyException, URISyntaxException, StorageException  {
    	CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).
    			createCloudQueueClient()
                .getQueueReference(this.debtPositionQueue);
        queue.createIfNotExists();

        this.getDebtPositionEntities(fileName, payments).forEach(msg -> {  
                this.logger.log(Level.INFO, () -> "[CuCsvService] push debt position in queue " + msg);
                try {
					queue.addMessage(new CloudQueueMessage(ObjectMapperUtils.writeValueAsString(msg)));
				} catch (JsonProcessingException | StorageException e) {
					logger.log(Level.SEVERE, () -> "[CuCsvService] exception : " + e.getMessage() + " " + e.getCause());
				}
        });
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
    
    
    private List<DebtPositionEntity> getDebtPositionEntities (String fileName, List<PaymentNotice> payments) {
    	List<DebtPositionEntity> debtPositionEntities = new ArrayList<>(); 
    	for (PaymentNotice p: payments) {
    		DebtPositionEntity e = new DebtPositionEntity(fileName, p.getId());
    		String iuv = this.generateIUV("cu", 47, 3);
    		e.setIuv(this.generateIUV("cu", 47, 3));
    		e.setIupd(this.generateIUPD(iuv));
    		e.setPaIdIstat(p.getPaIdIstat());
    		e.setPaIdCatasto(p.getPaIdCatasto());
    		e.setPaIdFiscalCode(p.getPaIdFiscalCode());
    		e.setPaIdCbill(p.getPaIdCBill());
    		e.setPaPecEmail(p.getPaPecEmail());
    		e.setPaReferentName(p.getPaReferentName());
    		e.setPaReferentEmail(p.getPaReferentEmail());
    		e.setDebtorIdFiscalCode(p.getDebtorFiscalCode());
    		e.setPaymentNoticeNumber(String.valueOf(p.getPaymentNoticeNumber()));
    		e.setNote(p.getNote());
    		e.setDebtorName(p.getDebtorName());
    		e.setDebtorEmail(p.getDebtorEmail());
    		e.setAmount(String.valueOf(p.getAmount()));
    		
    		// from ec_config
    		e.setFiscalCode("fiscalCode");
    		e.setCompanyName("company name");
    		e.setIban("iban");
    		
    	}
		return debtPositionEntities;
    }

    private String generateIUV(String idDominioPa, int segregationCode, int auxDigit) {
    	/*
    	String zone = "Europe/Paris";
		Long lastNumber = 1l;
		IuvNumber incrementalIuvNumber = null;
		//IuvNumber incrementalIuvNumber = incrementalIuvNumberRepository.findByIdDominioPaAndAnno(idDominioPa,
			//	LocalDateTime.now(ZoneId.of(this.zone)).getYear());
		if (incrementalIuvNumber != null) {
			lastNumber = (incrementalIuvNumber.getLastUsedNumber() + 1);
			incrementalIuvNumber.setLastUsedNumber(lastNumber);

		} else {

			incrementalIuvNumber = new IuvNumber();
			incrementalIuvNumber.setAnno(LocalDateTime.now(ZoneId.of(zone)).getYear());
			incrementalIuvNumber.setIdDominioPa(idDominioPa);
			incrementalIuvNumber.setLastUsedNumber(lastNumber);
		}
		//incrementalIuvNumberRepository.saveAndFlush(incrementalIuvNumber);

		IuvCodeGenerator iuvCodeGenerator = new IuvCodeGenerator.Builder().setAuxDigit(auxDigit)
				.setSegregationCode(segregationCode).build();

		IuvCodeBusiness.validate(iuvCodeGenerator);
		return auxDigit + IuvCodeBusiness.generateIUV(segregationCode, lastNumber + "");*/
    	byte[] array = new byte[17]; // length is bounded by 7
        new Random().nextBytes(array);
        return new String(array, Charset.forName("UTF-8"));

	}
    
    private String generateIUPD (String iuv) {
    	return "CU_2022_"+iuv;
    }
    
}
