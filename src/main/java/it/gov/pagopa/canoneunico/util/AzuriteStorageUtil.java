package it.gov.pagopa.canoneunico.util;


import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableRequestOptions;
import lombok.AllArgsConstructor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

@AllArgsConstructor
public class AzuriteStorageUtil {

    private final boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));

    private String storageConnectionString;
    private String flowsTable;
    private String containerBlobIn;
    private String containerBlobOut;

    // Create a new table
    public void createTable() throws URISyntaxException, InvalidKeyException, StorageException {
        if (debugAzurite) {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
            CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
            TableRequestOptions tableRequestOptions = new TableRequestOptions();
            tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance()); // disable retry to complete faster
            cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
            CloudTable table = cloudTableClient.getTableReference(flowsTable);

            table.createIfNotExists();
        }
    }

    // Create a new blob
    public void createBlobIn() {
        if (debugAzurite) {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(this.storageConnectionString).buildClient();
            BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerBlobIn);
            if (!container.exists()) {
                blobServiceClient.createBlobContainer(containerBlobIn);
            }
        }
    }

    public void createBlobOut() {
        if (debugAzurite) {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(this.storageConnectionString).buildClient();
            BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerBlobOut);
            if (!container.exists()) {
                blobServiceClient.createBlobContainer(containerBlobOut);
            }
        }
    }
}
