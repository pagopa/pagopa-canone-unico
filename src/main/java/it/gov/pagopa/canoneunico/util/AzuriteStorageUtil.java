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
import lombok.NoArgsConstructor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

@AllArgsConstructor
@NoArgsConstructor
public class AzuriteStorageUtil {

    private boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");


    // Create a new table
    public void createTable(String tableName) throws URISyntaxException, InvalidKeyException, StorageException {
        if (debugAzurite) {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
            CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
            TableRequestOptions tableRequestOptions = new TableRequestOptions();
            tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance());
            cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
            CloudTable table = cloudTableClient.getTableReference(tableName);

            table.createIfNotExists();
        }
    }

    // Create a new queue
    public void createQueue(String queueName) throws URISyntaxException, InvalidKeyException, StorageException {
        if (debugAzurite) {
            CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).createCloudQueueClient()
                    .getQueueReference(queueName);
            queue.createIfNotExists();
        }
    }

    // Create a new blob
    public void createBlob(String containerName) {
        if (debugAzurite) {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(this.storageConnectionString).buildClient();
            BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);
            if (!container.exists()) {
                blobServiceClient.createBlobContainer(containerName);
            }
        }
    }
}
