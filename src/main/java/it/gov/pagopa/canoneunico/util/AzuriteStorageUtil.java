package it.gov.pagopa.canoneunico.util;


import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.core.Logger;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableRequestOptions;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.model.CsvOutModel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class AzuriteStorageUtil {

    private boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private static final String INPUT_CONTAINER_NAME = "input";
    private static final String KEY_SEPARATOR = "_";

    // Create a new table
    public void createTable(String tableName) throws URISyntaxException, InvalidKeyException, StorageException {
        if (debugAzurite) {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
            CloudTableClient cloudTableClient = cloudStorageAccount.createCloudTableClient();
            TableRequestOptions tableRequestOptions = new TableRequestOptions();
            tableRequestOptions.setRetryPolicyFactory(RetryNoRetry.getInstance());
            cloudTableClient.setDefaultRequestOptions(tableRequestOptions);
            CloudTable table = cloudTableClient.getTableReference(tableName);
            try {
            	table.create();
            } catch (Exception e) {
            	Logger.info(new OperationContext(), "Table already exist:" + tableName);
            }
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

    public BinaryData downloadBlob(ExecutionContext context, String containerName, String blob) throws CanoneUnicoException {
        if(!debugAzurite) return null;

        context.getLogger().info(String.format("[AzuriteStorageUtil] Download blob %s from container %s", blob, containerName));

        try {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(this.storageConnectionString).buildClient();
            BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);
            if (!container.exists()) return null;
            BlobClient blobClient = container.getBlobClient(blob);
            if (!blobClient.exists()) return null;

            context.getLogger().info(String.format("[AzuriteStorageUtil][Downloaded Blob] Blob name %s, blob URL %s", blobClient.getBlobName(), blobClient.getBlobUrl()));

            return blobClient.downloadContent();
        } catch (BlobStorageException e) {
            throw new CanoneUnicoException("[AzureStorageUtil] BlobStorageException " + e.getMessage());
        }
    }

    // based on this format corp_blobName return CsvOutModel instance
    public static CsvOutModel getOutByBlobKey(String blobKey, List<List<String>> data) {
        String corporate = blobKey.substring(0, blobKey.indexOf(KEY_SEPARATOR));
        String filename = blobKey.substring(blobKey.indexOf(KEY_SEPARATOR)+1);
        return new CsvOutModel(corporate, filename, data);
    }

    // wrapper for method getBlobKey(String blobContainer, String blobName)
    public static String getBlobKey(BlobContainerItem blobContainer, BlobItem blobItem) {
        return getBlobKey(blobContainer.getName(), blobItem.getName().replace(INPUT_CONTAINER_NAME + "/", ""));
    }

    // key = corp_blobName
    public static String getBlobKey(String blobContainer, String blobName) {
        return blobContainer.concat(KEY_SEPARATOR).concat(blobName);
    }
}
