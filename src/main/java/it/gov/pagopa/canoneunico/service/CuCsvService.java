package it.gov.pagopa.canoneunico.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
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
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;



public class CuCsvService {
	
	private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String containerInputBlob = System.getenv("INPUT_CSV_BLOB");
    private String containerErrorBlob = System.getenv("ERROR_CSV_BLOB");
	private String debtPositionTable = System.getenv("DEBT_POSITIONS_TABLE");
	private String debtPositionQueue = System.getenv("DEBT_POSITIONS_QUEUE");
	private int batchSizeDebtPosQueue = 5;
	private int batchSizeDebtPosTable = 5;
	
    private Logger logger;
    

    public CuCsvService(Logger logger) {
        this.logger = logger;
    }
    
    public CuCsvService(String storageConnectionString, String containerInputBlob, String containerErrorBlob, String debtPositionTable, String debtPositionQueue, Logger logger) {
    	this.storageConnectionString = storageConnectionString;
    	this.containerInputBlob = containerInputBlob;
    	this.containerErrorBlob = containerErrorBlob;
    	this.debtPositionTable = debtPositionTable;
    	this.debtPositionQueue = debtPositionQueue;		
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
    	AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
    	azuriteStorageUtil.createBlob(containerErrorBlob);
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerErrorBlob);
		BlockBlobClient blockBlobClient = cont.getBlobClient(fileName).getBlockBlobClient();
		InputStream stream = new ByteArrayInputStream(content.getBytes());
		blockBlobClient.upload(stream, content.getBytes().length);
    }

    public void deleteCsv(String fileName) {
    	AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
    	azuriteStorageUtil.createBlob(containerInputBlob);
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerInputBlob);
		cont.getBlobClient(fileName).delete();
    }
    
    public List<DebtPositionEntity> saveDebtPosition(String fileName, List<PaymentNotice> payments) {

    	this.logger.log(Level.INFO, () -> "[CuCsvService] save debt position in table for file " + fileName);
    	

    	List<DebtPositionEntity> savedDebtPositionEntities = new ArrayList<>();

    	List<List<DebtPositionEntity>> partitionDebtPositionEntities = Lists.partition(this.getDebtPositionEntities(fileName, payments), batchSizeDebtPosTable);

    	// save debt positions partition in table 
    	IntStream.range(0, partitionDebtPositionEntities.size()).forEach(partitionAddIndex -> {
    		try {
    			this.addDebtPositionEntityList(partitionDebtPositionEntities.get(partitionAddIndex));
    			logger.log(Level.INFO, () -> "[CuCsvService] Azure Table Storage - Add for partition index " + partitionAddIndex + " executed."); 
    			savedDebtPositionEntities.addAll(partitionDebtPositionEntities.get(partitionAddIndex));
    		} catch (InvalidKeyException | CanoneUnicoException | URISyntaxException | StorageException e) {
    			logger.log(Level.SEVERE, () -> "[CuCsvService] Exception in add Azure Table Storage batch debt position entities: " + e.getMessage() + " " + e.getCause());
    		}
    	});    	 

    	return savedDebtPositionEntities;
    }
    
    public boolean pushDebtPosition(String fileName, List<DebtPositionEntity> debtPositionEntities) {
    	
    	this.logger.log(Level.INFO, () -> "[CuCsvService] push debt position in queue for file " + fileName);
    	
    	AtomicBoolean isAllMsgPushed = new AtomicBoolean(true);
    	
    	DebtPositionMessage debtPositionMessage = new DebtPositionMessage();
    	debtPositionMessage.setCsvFilename(fileName);
    	
    	List<List<DebtPositionRowMessage>> msgRows = Lists.partition(this.getDebtPositionQueueMsg(debtPositionEntities), batchSizeDebtPosQueue);	
    	
    	// push debt positions partition in queue 
        IntStream.range(0, msgRows.size()).forEach(partitionAddIndex -> {
            try {
            	debtPositionMessage.setRows(msgRows.get(partitionAddIndex));
                this.addDebtPositionMsg(debtPositionMessage);
                logger.log(Level.INFO, () -> "[CuCsvService] Azure Queue Storage - Add for partition index " + partitionAddIndex + " executed.");
            } catch (JsonProcessingException | StorageException | InvalidKeyException | URISyntaxException e) {
            	logger.log(Level.SEVERE, () -> "[CuCsvService] Exception in add Azure Queue Storage batch debt position queue msg: " + e.getMessage() + " " + e.getCause());
            	isAllMsgPushed.set(false);
            }
               
        });
        
        return isAllMsgPushed.get();
    }
    
    public void addDebtPositionMsg (DebtPositionMessage msg) throws InvalidKeyException, URISyntaxException, StorageException, JsonProcessingException {
    	
    	logger.log(Level.INFO, () -> "[CuCsvService] pushing debt position in queue ["+debtPositionQueue+"]: " + msg);
    	
    	AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
    	azuriteStorageUtil.createQueue(debtPositionQueue);
    	
    	CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).
    			createCloudQueueClient()
                .getQueueReference(debtPositionQueue);
    	
    	queue.addMessage(new CloudQueueMessage(ObjectMapperUtils.writeValueAsString(msg)));
    }
    
    public void addDebtPositionEntityList (List<DebtPositionEntity> debtPositionEntities) throws CanoneUnicoException, InvalidKeyException, URISyntaxException, StorageException  {
    	AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
    	azuriteStorageUtil.createTable(debtPositionTable);

    	CloudTable table = CloudStorageAccount.parse(storageConnectionString)
    			.createCloudTableClient()
    			.getTableReference(debtPositionTable);

    	TableBatchOperation batchOperation = new TableBatchOperation();

    	debtPositionEntities.forEach(debtPosition -> {
    		this.logger.log(Level.INFO, () -> "[CuCsvService] saving debt position in table ["+debtPositionTable+"]: " + debtPosition);
    		batchOperation.insert(debtPosition);
    	});

    	table.execute(batchOperation);
    }
    
    public String generateErrorCsv (String converted, DebtPositionValidationCsv csvValidationErrors)  {
    	
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
    	    String footer ="nLinesError/nTotLines:" + csvValidationErrors.getNumberInvalidRows()+"/"+csvValidationErrors.getTotalNumberRows();
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
    		
    		e.setStatus(Status.INSERTED.toString());
    		
    		debtPositionEntities.add(e);
    		
    	}
		return debtPositionEntities;
    }
    
    private List<DebtPositionRowMessage> getDebtPositionQueueMsg (List<DebtPositionEntity> debtPositionEntities) {
    	List<DebtPositionRowMessage> debtPositionMsgs = new ArrayList<>(); 
    	for (DebtPositionEntity e: debtPositionEntities) {
    		DebtPositionRowMessage row = new DebtPositionRowMessage();
    		row.setId(e.getRowKey());
    		row.setDebtorName(e.getDebtorName());
    		row.setDebtorEmail(e.getDebtorEmail());
    		row.setAmount(Long.parseLong(e.getAmount()));
    		row.setIuv(e.getIuv());
    		row.setIupd(e.getIupd());
    		row.setFiscalCode(e.getFiscalCode());
    		row.setCompanyName(e.getCompanyName());
    		row.setIban(e.getIban());
    		debtPositionMsgs.add(row);
    	}
		return debtPositionMsgs;
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
        return UUID.randomUUID().toString();

	}
    
    private String generateIUPD (String iuv) {
    	return "CU_2022_"+iuv;
    }
    
}
