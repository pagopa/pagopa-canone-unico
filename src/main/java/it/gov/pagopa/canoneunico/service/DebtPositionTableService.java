package it.gov.pagopa.canoneunico.service;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DebtPositionTableService {

    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String tableName = System.getenv("DEBT_POSITIONS_TABLE");
    private boolean debugAzurite = Boolean.parseBoolean(System.getenv("DEBUG_AZURITE"));

    private final Logger logger;

    public DebtPositionTableService(Logger logger) {
        this.logger = logger;
        createEnv();
    }

    public DebtPositionTableService(String storageConnectionString, String tableName, boolean debugAzurite, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.tableName = tableName;
        this.debugAzurite = debugAzurite;
        this.logger = logger;
        createEnv();
    }


    /**
     * @param filename     used as partition key
     * @param debtPosition elem of the message
     * @param status       to update
     * @param requestId
     */
    public void updateEntity(String filename, DebtPositionRowMessage debtPosition, boolean status, String requestId) {


        try {
            // get the table
            CloudTable table = CloudStorageAccount.parse(storageConnectionString).createCloudTableClient()
                    .getTableReference(this.tableName);

            // update the entity
            DebtPositionEntity entity = new DebtPositionEntity(filename, debtPosition.getId());
            entity.setStatus(status ? Status.CREATED.name() : Status.ERROR.name());

            var updateOperation = TableOperation.merge(entity);

            table.execute(updateOperation);

        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            this.logger.log(Level.SEVERE, () -> "[DebtPositionTableService ERROR][requestId=" + requestId + "][" + filename + "] Error " + e);
        }
    }

    public DebtPositionEntity getEntity(String filename, String id) {

        this.logger.log(Level.INFO, "[DebtPositionTableService] get entity ");

        try {
            // get the table
            CloudTable table = CloudStorageAccount.parse(storageConnectionString).createCloudTableClient()
                    .getTableReference(this.tableName);

            TableQuery<DebtPositionEntity> query = TableQuery.from(DebtPositionEntity.class)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, filename))
                    .where(TableQuery.generateFilterCondition("RowKey", TableQuery.QueryComparisons.EQUAL, id));

            Iterable<DebtPositionEntity> result = table.execute(query);

            return result.iterator().next();

        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            this.logger.log(Level.SEVERE, () -> "[DebtPositionTableService ERROR] Error " + e);
        }
        return null;
    }


    /**
     * @param debtPositions insert in the table a list of {@link DebtPositionEntity}
     */
    public void batchInsert(List<DebtPositionEntity> debtPositions) {

        try {
            CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                    .createCloudTableClient()
                    .getTableReference(this.tableName);

            TableBatchOperation batchOperation = new TableBatchOperation();
            debtPositions.forEach(batchOperation::insert);

            table.execute(batchOperation);

        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            logger.log(Level.SEVERE, () -> "[DebtPositionTableService ERROR] Error during batch insert " + e.getLocalizedMessage());
        }
    }

    private void createEnv() {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil(debugAzurite, storageConnectionString);
        try {
            azuriteStorageUtil.createTable(tableName);
        } catch (Exception e) {
            this.logger.severe(String.format("[DebtPositionTableService ERROR] Problem to create table or queue: %s", e.getMessage()));
        }
    }


}
