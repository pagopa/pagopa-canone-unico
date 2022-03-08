package it.gov.pagopa.canoneunico.service;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.activation.DataHandler;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.implementation.models.Logging;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class DebtPositionServiceIntegrationTest {

  @ClassRule @Container
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

    assertTrue(pks.size() == noBlob);
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

    List<String> pks = new ArrayList<>();
    pks.add("csv1");
    pks.add("csv2");

    List<List<List<String>>> data = debtPositionService.getDebtPositionListByPk(pks);
    // Output expected NoSuchElementExceptions.

    assertTrue(data.get(0).size() == 0);
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

        DebtPositionEntity debtPositionEntity;
        for (int j= 0; j < 10; j++) {
          for (int i = 0; i < 5; i++) {
            debtPositionEntity = new DebtPositionEntity("csv" + j, Integer.toString(i));
            debtPositionEntity.setStatus(Status.CREATED.toString());
            // CSV
            debtPositionEntity.setPaIdIstat("paIdIstat");
            debtPositionEntity.setPaIdCatasto("paIdCatasto");
            debtPositionEntity.setPaIdFiscalCode("paIdFiscalCode");
            debtPositionEntity.setPaIdCbill("paIdCbill");
            debtPositionEntity.setPaPecEmail("paPecEmail");
            debtPositionEntity.setPaReferentEmail("paReferentEmail");
            debtPositionEntity.setPaReferentName("paReferentName");
            debtPositionEntity.setDebtorIdFiscalCode("debtorIdFiscalCode");
            debtPositionEntity.setPaymentNoticeNumber("paymentNoticeNumber");
            debtPositionEntity.setNote("note");

            debtPositionEntity.setDebtorName("debtorName");
            debtPositionEntity.setDebtorEmail("debtorEmail");
            debtPositionEntity.setAmount("amount"); // Long

            // generated
            debtPositionEntity.setIuv("iub");
            debtPositionEntity.setIupd("iudp");

            // EC config
            debtPositionEntity.setFiscalCode("fiscalCode");
            debtPositionEntity.setCompanyName("companyName");
            debtPositionEntity.setIban("iban");

            table.execute(TableOperation.insert(debtPositionEntity));
          }
        }

    List<String> pks = new ArrayList<>();
    pks.add("csv1");
    pks.add("csv2");

    List<List<List<String>>> data = debtPositionService.getDebtPositionListByPk(pks);
    // Output expected NoSuchElementExceptions.

    assertTrue(data.get(0).size() == 0);
  }

  @Test
  void uploadOutFileTest() {}
}
