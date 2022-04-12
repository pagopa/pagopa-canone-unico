package it.gov.pagopa.canoneunico.service;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.xml.datatype.DatatypeConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

@Testcontainers
class DebtPositionServiceIntegrationTest {

    @ClassRule
    @Container
    public static GenericContainer<?> azurite =
            new GenericContainer<>(
                    DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest"))
                    .withExposedPorts(10001, 10002, 10000);

    Logger logger = Logger.getLogger("testlogging");

    String storageConnectionString =
            String.format(
                    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://%s:%s/devstoreaccount1;QueueEndpoint=http://%s:%s/devstoreaccount1;BlobEndpoint=http://%s:%s/devstoreaccount1",
                    azurite.getContainerIpAddress(),
                    azurite.getMappedPort(10002),
                    azurite.getContainerIpAddress(),
                    azurite.getMappedPort(10001),
                    azurite.getContainerIpAddress(),
                    azurite.getMappedPort(10000));

    String debtPositionsTable = "debtbositionstable";
    String containerBlobIn = "containerblobin";
    String containerBlobOut = "containerblobout";

    DebtPositionService debtPositionService;

    @Test
    void getCsvFilePkTest()
            throws ParseException, DatatypeConfigurationException, InvalidKeyException,
            URISyntaxException {

        debtPositionService =
                spy(
                        new DebtPositionService(
                                storageConnectionString,
                                debtPositionsTable,
                                containerBlobIn,
                                containerBlobOut,
                                logger));

        BlobServiceClient blobServiceClient =
                new BlobServiceClientBuilder().connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient containerClient = blobServiceClient.createBlobContainer(containerBlobIn);

        String initialString = "test-text";
        InputStream targetStream;
        BlobClient blobClient;
        int noBlob = 10;

        for (int i = 0; i < noBlob; i++) {
            blobClient = containerClient.getBlobClient("csv" + i + ".csv");
            targetStream = new ByteArrayInputStream(initialString.getBytes());
            blobClient.upload(BinaryData.fromStream(targetStream));
        }

        List<String> pks = debtPositionService.getCsvFilePk();

        List<String> fileNames =
                containerClient.listBlobs().stream().map(BlobItem::getName).collect(Collectors.toList());

        assertEquals(noBlob, pks.size());
    }

    @Test
    void getDebtPositionListByPkTestKo()
            throws URISyntaxException, InvalidKeyException, StorageException {
        debtPositionService =
                spy(
                        new DebtPositionService(
                                storageConnectionString,
                                debtPositionsTable,
                                containerBlobIn,
                                containerBlobOut,
                                logger));

        CloudTable table =
                CloudStorageAccount.parse(storageConnectionString)
                        .createCloudTableClient()
                        .getTableReference(debtPositionsTable);
        table.createIfNotExists();

        List<String> pks = new ArrayList<>();
        pks.add("csv1");
        pks.add("csv2");

        List<List<List<String>>> data = debtPositionService.getDebtPositionListByPk(pks);
        // Output expected NoSuchElementExceptions.

        assertEquals(data.get(0).size(), 0);
    }

    @Test
    void getDebtPositionListByPkTestOk()
            throws URISyntaxException, InvalidKeyException, StorageException {
        debtPositionService =
                spy(
                        new DebtPositionService(
                                storageConnectionString,
                                debtPositionsTable,
                                containerBlobIn,
                                containerBlobOut,
                                logger));

        CloudTable table =
                CloudStorageAccount.parse(storageConnectionString)
                        .createCloudTableClient()
                        .getTableReference(debtPositionsTable);
//    table.createIfNotExists();

        DebtPositionEntity debtPositionEntity;
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 5; i++) {
                debtPositionEntity = new DebtPositionEntity("csv" + j, Integer.toString(i));
                if (j == 2 && i == 2) {
                    debtPositionEntity.setStatus(Status.INSERTED.toString());
                } else {
                    debtPositionEntity.setStatus(Status.CREATED.toString());
                }
                // CSV
                debtPositionEntity.setPaIdIstat("paIdIstat");
                debtPositionEntity.setPaIdCatasto("paIdCatasto");
                debtPositionEntity.setPaIdFiscalCode("paIdFiscalCode");
                debtPositionEntity.setPaIdCbill("paIdCbill");
                debtPositionEntity.setPaPecEmail("paPecEmail");
                debtPositionEntity.setPaReferentEmail("paReferentEmail");
                debtPositionEntity.setPaReferentName("paReferentName");
                debtPositionEntity.setDebtorIdFiscalCode("debtorIdFiscalCode");
                debtPositionEntity.setNote("note");

                debtPositionEntity.setDebtorName("debtorName");
                debtPositionEntity.setDebtorEmail("debtorEmail");
                debtPositionEntity.setAmount("amount"); // Long

                // generated
                debtPositionEntity.setPaymentNoticeNumber("paymentNoticeNumber");
                debtPositionEntity.setIupd("iudp");

                // EC config
                debtPositionEntity.setFiscalCode("fiscalCode");
                debtPositionEntity.setCompanyName("companyName");
                debtPositionEntity.setIban("iban");

                table.execute(TableOperation.insert(debtPositionEntity));
            }
        }

        List<String> pks = new ArrayList<>();
        pks.add("csv0"); // all CREATED
        pks.add("csv1"); // all CREATED
        pks.add("csv2"); // not all CREATED
        pks.add("csv3"); // doesn't exists

        List<List<List<String>>> data = debtPositionService.getDebtPositionListByPk(pks);
        // Output expected NoSuchElementExceptions.

        assertEquals(data.get(0).size(), 5); // all CREATED
        assertEquals(data.get(1).size(), 5); // all CREATED
        assertEquals(data.get(2).size(), 0); // not all CREATED
        assertEquals(data.get(3).size(), 0); // doesn't exists
    }

    @Test
    void getDebtPositionSkippedTestOk() throws URISyntaxException, InvalidKeyException, StorageException {
        // init table
        debtPositionService = spy(new DebtPositionService(
                storageConnectionString,
                debtPositionsTable,
                containerBlobIn,
                containerBlobOut,
                logger));

        CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                .createCloudTableClient()
                .getTableReference(debtPositionsTable);

        //insert data into table
        var debtPositionEntity1 = getMockDebtPosition("1", Status.CREATED.name());
        var debtPositionEntity2 = getMockDebtPosition("2", Status.SKIPPED.name());

        table.execute(TableOperation.insert(debtPositionEntity1));
        table.execute(TableOperation.insert(debtPositionEntity2));


        List<String> pks = new ArrayList<>();
        pks.add("csv"); // all CREATED

        var data = debtPositionService.getDebtPositionListByPk(pks);

        assertEquals(2, data.get(0).size()); // all OK

    }

    @NotNull
    private DebtPositionEntity getMockDebtPosition(String id, String status) {
        DebtPositionEntity debtPositionEntity = new DebtPositionEntity("csv", id);
        debtPositionEntity.setStatus(status);
        // CSV
        debtPositionEntity.setPaIdIstat("paIdIstat");
        debtPositionEntity.setPaIdCatasto("paIdCatasto");
        debtPositionEntity.setPaIdFiscalCode("paIdFiscalCode");
        debtPositionEntity.setPaIdCbill("paIdCbill");
        debtPositionEntity.setPaPecEmail("paPecEmail");
        debtPositionEntity.setPaReferentEmail("paReferentEmail");
        debtPositionEntity.setPaReferentName("paReferentName");
        debtPositionEntity.setDebtorIdFiscalCode("debtorIdFiscalCode");
        debtPositionEntity.setNote("note");

        debtPositionEntity.setDebtorName("debtorName");
        debtPositionEntity.setDebtorEmail("debtorEmail");
        debtPositionEntity.setAmount("amount"); // Long

        // generated
        debtPositionEntity.setPaymentNoticeNumber("paymentNoticeNumber");
        debtPositionEntity.setIupd("iudp");

        // EC config
        debtPositionEntity.setFiscalCode("fiscalCode");
        debtPositionEntity.setCompanyName("companyName");
        debtPositionEntity.setIban("iban");
        return debtPositionEntity;
    }

    @Test
    void uploadOutFileTest() throws FileNotFoundException {

        debtPositionService =
                spy(
                        new DebtPositionService(
                                storageConnectionString,
                                debtPositionsTable,
                                containerBlobIn,
                                containerBlobOut,
                                logger));

        BlobServiceClient blobServiceClient =
                new BlobServiceClientBuilder().connectionString(this.storageConnectionString).buildClient();

        BlobContainerClient containerClientBlobIn = blobServiceClient.getBlobContainerClient(containerBlobIn);
        if (!containerClientBlobIn.exists()) {
            containerClientBlobIn = blobServiceClient.createBlobContainer(containerBlobIn);
        }


        String initialString = "test-text";
        InputStream targetStream;
        BlobClient blobClient;

        String csvFileName = "filename.csv";

        blobClient = containerClientBlobIn.getBlobClient(csvFileName);
        targetStream = new ByteArrayInputStream(initialString.getBytes());
        blobClient.upload(BinaryData.fromStream(targetStream));

        BlobContainerClient containerClientBlobOut = blobServiceClient.createBlobContainer(containerBlobOut);

        List<List<String>> dataLines = new ArrayList<>();
        List<String> row1 = new ArrayList<>();
        row1.add("attr1");
        row1.add("attr2");
        row1.add("attr3");
        List<String> row2 = new ArrayList<>();
        row2.add("attr1");
        row2.add("attr2");
        row2.add("attr3");
        dataLines.add(row1);
        dataLines.add(row2);

        debtPositionService.uploadOutFile(csvFileName, dataLines);


        List<String> fileNamesOut = containerClientBlobOut.listBlobs().stream().map(BlobItem::getName)
                .collect(Collectors.toList());

        List<String> fileNamesIn = containerClientBlobIn.listBlobs().stream().map(BlobItem::getName)
                .collect(Collectors.toList());

        assertTrue(fileNamesOut.contains(csvFileName));
        assertTrue(!fileNamesIn.contains(csvFileName));

    }
}
