package it.gov.pagopa.canoneunico.service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableQuery;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DebtPositionService {

    private String storageConnectionString;
    private String debtPositionsTable;
    private String containerBlobIn;
    private String containerBlobOut;
    private Logger logger;

    private final String csvHd = "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_email;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;debtor_name;debtor_email;payment_notice_number;note";

    public DebtPositionService(String storageConnectionString, String debtPositionsTable, String containerBlobIn, String containerBlobOut, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.debtPositionsTable = debtPositionsTable;
        this.containerBlobIn = containerBlobIn;
        this.containerBlobOut = containerBlobOut;
        this.logger = logger;
    }

    public List<String> getCsvFilePk() {
        // try to create Azure table and queue
        createEnv();

        this.logger.info("[OrganizationsService] Processing organization list");

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();

        BlobContainerClient client = blobServiceClient.getBlobContainerClient(this.containerBlobIn);

//        client.listBlList<String>obs().forEach(blob ->
//                System.out.printf("Name: %s, Directory? %b%n", blob.getName(), blob.isPrefix()));

        return client.listBlobs().stream()
                .map(item -> item.getName())
                .collect(Collectors.toList());

    }

    public List<List<String>> getDebtPositionListByPk(List<String> pks) throws URISyntaxException, InvalidKeyException, StorageException {
        CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                .createCloudTableClient()
                .getTableReference(this.debtPositionsTable);


        return pks.stream()
                .map(pk -> {
                    List<String> rowList = new ArrayList<>();
                    for (DebtPositionEntity entity : table.execute(TableQuery.from(DebtPositionEntity.class).where((TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, pk))))) {
                        Collections.addAll(rowList,
                                entity.getRowKey(),
                                entity.getPaIdIstat(),
                                entity.getPaIdCatasto(),
                                entity.getPaIdFiscalCode(),
                                entity.getPaIdCbill(),
                                entity.getPaPecEmail(),
                                entity.getPaReferentEmail(),
                                entity.getAmount(),
                                entity.getDebtorIdFiscalCode(),
                                entity.getDebtorName(),
                                entity.getDebtorEmail(),
                                entity.getPaymentNoticeNumber(),
                                entity.getNote());
                    }
                    return  rowList;
                })
                .collect(Collectors.toList());

    }

    public void writeToFile(String csvFileName, List<List<String>> dataLines) throws IOException {
        File csvOutputFile = new File(csvFileName);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            dataLines.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        }
    }

    private String convertToCSV(List<String> data) {
        return data.stream().collect(Collectors.joining("-"));

    }

    private void createEnv() {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil(storageConnectionString, debtPositionsTable, containerBlobIn, containerBlobOut);
        try {
            azuriteStorageUtil.createTable();
            azuriteStorageUtil.createBlobIn();
            azuriteStorageUtil.createBlobOut();
        } catch (Exception e) {
            this.logger.severe(String.format("[AzureStorage] Problem to create table or queue: %s", e.getMessage()));
        }
    }


}
