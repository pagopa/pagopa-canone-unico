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
import com.microsoft.azure.storage.table.TableQuery;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DebtPositionService {

    private static final String CSV_HEAD =
            "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_email;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;debtor_name;debtor_email;payment_notice_number;note";
    private static final String CU_AUX_DIGIT = System.getenv("CU_AUX_DIGIT");
    public static final String STATUS = "Status";
    private final boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));
    private final String storageConnectionString;
    private final String debtPositionsTable;
    private final String containerBlobIn;
    private final String containerBlobOut;
    private final Logger logger;

    public DebtPositionService(
            String storageConnectionString,
            String debtPositionsTable,
            String containerBlobIn,
            String containerBlobOut,
            Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.debtPositionsTable = debtPositionsTable;
        this.containerBlobIn = containerBlobIn;
        this.containerBlobOut = containerBlobOut;
        this.logger = logger;
    }

    public List<String> getCsvFilePk() {
        // try to create Azure table and queue
        createEnv();

        this.logger.fine("[DebtPositionService] Processing organization list");

        BlobServiceClient blobServiceClient =
                new BlobServiceClientBuilder().connectionString(this.storageConnectionString).buildClient();

        BlobContainerClient client = blobServiceClient.getBlobContainerClient(this.containerBlobIn);

        return client.listBlobs()
                .stream()
                .map(BlobItem::getName)
                .collect(Collectors.toList());
    }

    public List<List<List<String>>> getDebtPositionListByPk(List<String> pks)
            throws URISyntaxException, InvalidKeyException, StorageException {
        CloudTable table =
                CloudStorageAccount.parse(storageConnectionString)
                        .createCloudTableClient()
                        .getTableReference(this.debtPositionsTable);

        return pks.stream()
                .map(
                        pk -> {
                            List<List<String>> rowsList = new ArrayList<>();

                            String partitionFilter =
                                    TableQuery.generateFilterCondition(
                                            "PartitionKey", TableQuery.QueryComparisons.EQUAL, pk);
                            String stateFilterCreated =
                                    TableQuery.generateFilterCondition(
                                            STATUS, TableQuery.QueryComparisons.EQUAL, Status.CREATED.toString());
                            String stateFilterSkipped =
                                    TableQuery.generateFilterCondition(
                                            STATUS, TableQuery.QueryComparisons.EQUAL, Status.SKIPPED.toString());
                            String stateFilterInserted =
                                    TableQuery.generateFilterCondition(
                                            STATUS, TableQuery.QueryComparisons.EQUAL, Status.INSERTED.toString());
                            String stateFilterError =
                                    TableQuery.generateFilterCondition(
                                            STATUS, TableQuery.QueryComparisons.EQUAL, Status.ERROR.toString());

                            String combinedFilterCreated =
                                    TableQuery.combineFilters(
                                            stateFilterCreated, TableQuery.Operators.OR, stateFilterSkipped);
                            String combinedFilterOk =
                                    TableQuery.combineFilters(
                                            partitionFilter, TableQuery.Operators.AND, combinedFilterCreated);

                            String combinedFilterInsertedOrError =
                                    TableQuery.combineFilters(
                                            stateFilterInserted, TableQuery.Operators.OR, stateFilterError);
                            String combinedNotOk =
                                    TableQuery.combineFilters(
                                            partitionFilter, TableQuery.Operators.AND, combinedFilterInsertedOrError);

                            boolean isExistInsertedOrError;

                            try {
                                isExistInsertedOrError =
                                        (table.execute(TableQuery.from(DebtPositionEntity.class).where(combinedNotOk)))
                                                .iterator()
                                                .hasNext();
                            } catch (NoSuchElementException exception) {
                                return rowsList;
                            }

                            // not all entity of pk are CREATED
                            if (Boolean.TRUE.equals(isExistInsertedOrError)) {
                                return rowsList;
                            }

                            for (DebtPositionEntity entity :
                                    table.execute(
                                            TableQuery.from(DebtPositionEntity.class).where(combinedFilterOk))) {
                                List<String> rowsItem = new ArrayList<>();
                                Collections.addAll(
                                        rowsItem,
                                        deNull(entity.getRowKey()),
                                        deNull(entity.getPaIdIstat()),
                                        deNull(entity.getPaIdCatasto()),
                                        deNull(entity.getPaIdFiscalCode()),
                                        deNull(entity.getPaIdCbill()),
                                        deNull(entity.getPaPecEmail()),
                                        deNull(entity.getPaReferentEmail()),
                                        deNull(entity.getPaReferentName()),
                                        deNull(entity.getAmount()),
                                        deNull(entity.getDebtorIdFiscalCode()),
                                        deNull(entity.getDebtorName()),
                                        deNull(entity.getDebtorEmail()),
                                        CU_AUX_DIGIT + entity.getPaymentNoticeNumber(),  // <AugDigit><codice segregazione(2n)><IUV base(13n)><IUV check digit(2n)>
                                        deNull(entity.getNote()));
                                rowsList.add(rowsItem);
                            }
                            return rowsList;
                        })
                .collect(Collectors.toList());
    }

    private String deNull(Object item) {
        return item != null ? item.toString() : "";
    }

    public void uploadOutFile(String csvFileName, List<List<String>> dataLines)
            throws FileNotFoundException {
        // insert blob in OUTPUT container
        BlobServiceClient blobServiceClient =
                new BlobServiceClientBuilder().connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient containerBlobOutClient =
                blobServiceClient.getBlobContainerClient(this.containerBlobOut);
        BlobClient blobClient = containerBlobOutClient.getBlobClient(csvFileName);

        File csvOutputFile = new File(csvFileName);

        dataLines.add(0, List.of(CSV_HEAD.split(";")));
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            dataLines.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        }

        blobClient.upload(BinaryData.fromStream(new FileInputStream(csvOutputFile)));

        logger.log(Level.INFO, () -> "[CuGenerateOutputCsvBatchFunction][" + csvFileName + "] Created output file in OutputStorage: " + csvOutputFile);

        // delete blob in INPUT container
        BlobContainerClient containerBlobInClient =
                blobServiceClient.getBlobContainerClient(this.containerBlobIn);
        BlobClient blobInClient = containerBlobInClient.getBlobClient(csvFileName);
        blobInClient.delete();
        logger.log(Level.INFO, () -> "[CuGenerateOutputCsvBatchFunction][" + csvFileName + "] Deleted file in InputStorage: " + csvFileName);
    }

    private String convertToCSV(List<String> data) {
        return String.join(";", data);
    }

    private void createEnv() {
        AzuriteStorageUtil azuriteStorageUtil =
                new AzuriteStorageUtil(debugAzurite, storageConnectionString);
        try {
            azuriteStorageUtil.createTable(debtPositionsTable);
            azuriteStorageUtil.createBlob(containerBlobIn);
            azuriteStorageUtil.createBlob(containerBlobOut);
        } catch (Exception e) {
            this.logger.severe(
                    String.format("[AzureStorage] Problem to create table: %s", e.getMessage()));
        }
    }
}
