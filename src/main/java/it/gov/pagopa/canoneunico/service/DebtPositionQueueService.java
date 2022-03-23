package it.gov.pagopa.canoneunico.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DebtPositionQueueService {

    private boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));
    private String queueName = System.getenv("DEBT_POSITIONS_QUEUE");
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private final String timeToLiveInSeconds = System.getenv("QUEUE_TIME_TO_LIVE");
    private final String initialVisibilityDelayInSeconds = System.getenv("QUEUE_DELAY");

    private final Logger logger;

    public DebtPositionQueueService(Logger logger) {
        this.logger = logger;
        createEnv();
    }

    public DebtPositionQueueService(String storageConnectionString, String queueName, boolean debugAzurite, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.queueName = queueName;
        this.debugAzurite = debugAzurite;
        this.logger = logger;
        createEnv();
    }

    public void insertMessage(DebtPositionMessage msg) {

        try {
            logger.log(Level.INFO, () -> "[DebtPositionQueueService] pushing debt position in queue [" + queueName + "]: " + msg);

            AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
            azuriteStorageUtil.createQueue(queueName);

            CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).
                    createCloudQueueClient()
                    .getQueueReference(queueName);

            int timeToLive = timeToLiveInSeconds != null ? Integer.parseInt(timeToLiveInSeconds) : 60;
            int initialVisibilityDelay = initialVisibilityDelayInSeconds != null ? Integer.parseInt(initialVisibilityDelayInSeconds) : 0;
            queue.addMessage(new CloudQueueMessage(ObjectMapperUtils.writeValueAsString(msg)), timeToLive, initialVisibilityDelay, null, null);
        } catch (URISyntaxException | StorageException | InvalidKeyException | JsonProcessingException e) {
            this.logger.log(Level.SEVERE, () -> "[DebtPositionQueueService ERROR] Error " + e);
        }
    }

    private void createEnv() {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil(debugAzurite, storageConnectionString);
        try {
            azuriteStorageUtil.createQueue(queueName);
        } catch (Exception e) {
            this.logger.severe(String.format("[DebtPositionTableService ERROR] Problem to create queue: %s", e.getMessage()));
        }
    }
}
