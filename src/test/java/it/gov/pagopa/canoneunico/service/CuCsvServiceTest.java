package it.gov.pagopa.canoneunico.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableRequestOptions;
import com.opencsv.bean.CsvToBean;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;

@Testcontainers
class CuCsvServiceTest {

	@ClassRule @Container
	  public static GenericContainer<?> azurite =
	      new GenericContainer<>(
	              DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest"))
	          .withExposedPorts(10001, 10002, 10000);


    String storageConnectionString =
    	      String.format(
    	          "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://%s:%s/devstoreaccount1;QueueEndpoint=http://%s:%s/devstoreaccount1;BlobEndpoint=http://%s:%s/devstoreaccount1",
    	          azurite.getContainerIpAddress(),
    	          azurite.getMappedPort(10002),
    	          azurite.getContainerIpAddress(),
    	          azurite.getMappedPort(10001),
    	          azurite.getContainerIpAddress(),
    	          azurite.getMappedPort(10000));


    @Test
    void parseCsv() {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "debtPositionQ", logger));
        
        StringWriter csv = new StringWriter();
        
        String headers = "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_email;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;debtor_name;debtor_email;payment_notice_number;note";
        String row = "1;;C123;;;;;;383700;123456;Spa;spa@pec.spa.it;;";
        
        csv.append(headers);
        csv.append(System.lineSeparator());
        csv.append(row);
      
        CsvToBean<PaymentNotice> csvToBean = csvService.parseCsv(csv.toString());
        assertNotNull(csvToBean);
        assertEquals(1, csvToBean.parse().size());
        
    }
    
    @Test
    void uploadCsv() {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "debtPositionQ", logger));
        
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient container = blobServiceClient.getBlobContainerClient("error");
        if (!container.exists()) {
            blobServiceClient.createBlobContainer("error");
        }
        csvService.uploadCsv("fileName.txt", "test content string");
        // se arrivo a questa chiamata l'upload è andato a buon fine
        assertTrue(true);
        
    }
    
    @Test
    void deleteCsv() {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "debtPositionQ", logger));
        
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient container = blobServiceClient.getBlobContainerClient("input");
        if (!container.exists()) {
            blobServiceClient.createBlobContainer("input");
        }
		BlockBlobClient blockBlobClient = container.getBlobClient("fileName.txt").getBlockBlobClient();
        InputStream stream = new ByteArrayInputStream("test content string".getBytes());
        blockBlobClient.upload(stream, "test content string".getBytes().length);
        
        csvService.deleteCsv("fileName.txt");
        // se arrivo a questa chiamata la delete è andata a buon fine
        assertTrue(true);
        
    }
    
    @Test
    void saveDebtPosition() throws InvalidKeyException, URISyntaxException, StorageException {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "debtPositionT", "iuv", "47", logger));
        
        CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
        TableRequestOptions tableRequestOptions = new TableRequestOptions();
        tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance());
        cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
        
        try {
        	CloudTable table = cloudTableClient.getTableReference("debtPositionT");
            table.createIfNotExists();
            table = cloudTableClient.getTableReference("iuv");
            table.createIfNotExists();
        } catch (Exception e) {
        	logger.info("Table already exist");
        }
        
        
        List<PaymentNotice> payments = new ArrayList<>();
        PaymentNotice p = new PaymentNotice();
        p.setId("1");
        p.setAmount(0);
        p.setPaIdFiscalCode("paFiscalCode");
        payments.add(p);
        List<DebtPositionEntity> savedEntities = csvService.saveDebtPosition("fileName", payments);
        assertNotNull(savedEntities);
        assertEquals(1, savedEntities.size());
        
    }
    
    @Test
    void pushDebtPosition() throws InvalidKeyException, URISyntaxException, StorageException {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "queue", logger));
        
        CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).createCloudQueueClient()
                .getQueueReference("queue");
        queue.createIfNotExists();
        
        List<DebtPositionEntity> savedEntities = new ArrayList<>();
        DebtPositionEntity e = new DebtPositionEntity();
        e.setPartitionKey("filename_0000.csv");
        e.setRowKey("1");
        e.setDebtorName("name");
        e.setDebtorEmail("email");
        e.setAmount("0");
        e.setIuv("iuv");
        e.setIupd("iupd");
        e.setFiscalCode("fiscalcode");
        e.setCompanyName("companyname");
        e.setIban("iban");
        e.setStatus(Status.INSERTED.toString());
        savedEntities.add(e);

        boolean isAllPushed = csvService.pushDebtPosition("filename_0000.csv", savedEntities);
        assertTrue(isAllPushed);
        
    }
    
    @Test
    void addDebtPositionEntityList() throws InvalidKeyException, URISyntaxException, StorageException {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "debtPositionQ", logger));
        
        CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
        TableRequestOptions tableRequestOptions = new TableRequestOptions();
        tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance());
        cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
        CloudTable table = cloudTableClient.getTableReference("debtPositionT");
        try {
        	table.create();
        }
        catch (StorageException e) {
        	logger.log(Level.INFO, "table already exist");
        }
        
        List<DebtPositionEntity> entities = new ArrayList<>();
        DebtPositionEntity e = new DebtPositionEntity();
        e.setPartitionKey("filename_0000.csv");
        e.setRowKey("1");
        e.setDebtorName("name");
        e.setDebtorEmail("email");
        e.setAmount("0");
        e.setIuv("iuv");
        e.setIupd("iupd");
        e.setFiscalCode("fiscalcode");
        e.setCompanyName("companyname");
        e.setIban("iban");
        e.setStatus(Status.INSERTED.toString());
        entities.add(e);

        csvService.addDebtPositionEntityList(entities);
        // se arrivo a questa verifica l'esecuzione è andata a buon fine
        assertTrue(true);
        
    }
    
    @Test
    void addDebtPositionMsg() throws InvalidKeyException, URISyntaxException, StorageException, JsonProcessingException {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "queue", logger));
        
        CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).createCloudQueueClient()
                .getQueueReference("queue");
        queue.createIfNotExists();
        
        DebtPositionMessage debtPositionMessage = new DebtPositionMessage();
    	debtPositionMessage.setCsvFilename("filename_002.csv");
        
    	List<DebtPositionRowMessage> rows = new ArrayList<>();
    	DebtPositionRowMessage r = new DebtPositionRowMessage();
        r.setId("001");
        r.setDebtorName("name");
        r.setDebtorEmail("email@email.it");
        r.setAmount(0L);
        r.setIuv("iuv");
        r.setIupd("iupd");
        r.setFiscalCode("fiscalcode");
        r.setCompanyName("companyname");
        r.setIban("iban");
        rows.add(r);
        debtPositionMessage.setRows(rows);

        csvService.addDebtPositionMsg(debtPositionMessage);
        // se arrivo a questa verifica l'esecuzione è andata a buon fine
        assertTrue(true);
        
    }
    
    @Test
    void getValidIUV() throws InvalidKeyException, URISyntaxException, StorageException {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "debtPositionT", "iuv", "47", logger));
        
        CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
        TableRequestOptions tableRequestOptions = new TableRequestOptions();
        tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance());
        cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
        CloudTable table = cloudTableClient.getTableReference("iuv");
        table.createIfNotExists();
        
        String iuv = csvService.getValidIUV("fiscal-code", 47);
        assertNotNull(iuv); 
        assertEquals(17, iuv.getBytes().length);
        
    }
    
    @Test
    void generateIncrementalIUV() throws InvalidKeyException, URISyntaxException, StorageException {
        Logger logger = Logger.getLogger("testlogging");
        var csvService = spy(new CuCsvService(storageConnectionString, "debtPositionT", "iuv", "47", logger));        
        String iuv = csvService.generateIncrementalIUV(47,1);
        assertNotNull(iuv); 
        assertEquals(17, iuv.getBytes().length);
        
    }
    
    
    @Test
    void generateErrorCsv() {
        Logger logger = Logger.getLogger("testlogging");

        var csvService = spy(new CuCsvService(storageConnectionString, "input", "error", "debtPositionT", "debtPositionQ", logger));
        
        StringWriter csv = new StringWriter();
        
        String headers = "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_email;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;debtor_name;debtor_email;payment_notice_number;note";
        String row = "1;;C123;;;;;;383700;123456;Spa;spa@pec.spa.it;;";
        
        csv.append(headers);
        csv.append(System.lineSeparator());
        csv.append(row);
        
        
        
        DebtPositionErrorRow rowErr = new DebtPositionErrorRow();
        rowErr.setRowNumber(2);
        rowErr.getErrorsDetail().add("error");
        DebtPositionValidationCsv validCsv = new DebtPositionValidationCsv();
        validCsv.getErrorRows().add(rowErr);
        
        
        String errorFile = csvService.generateErrorCsv(csv.toString(), validCsv);
        assertNotNull(errorFile);
    }
    
    
}
