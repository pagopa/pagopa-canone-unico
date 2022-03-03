package it.gov.pagopa.canoneunico.service;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;
import lombok.AllArgsConstructor;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@AllArgsConstructor
public class DebtPositionTableService {

    private static final int BATCH_SIZE = 2;
    private final String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private final String tableName = System.getenv("DEBT_POSITIONS_TABLE");
    private Logger logger;


    public void updateEntity(String filename, DebtPositionRowMessage debtPosition, boolean status) {
        createEnv();

        this.logger.log(Level.INFO, "[DebtPositionTableService] START storing ");

        try {
            CloudTable table = CloudStorageAccount.parse(storageConnectionString).createCloudTableClient()
                    .getTableReference(this.tableName);

//            TableOperation retrieve = TableOperation.retrieve(filename, debtPosition.getId(), DebtPositionEntity.class);
//            TableResult result = table.execute(retrieve);
//            var entity = (DebtPositionEntity) result.getResult();
            DebtPositionEntity entity = new DebtPositionEntity(filename, debtPosition.getId());
            entity.setStatus(status ? "CREATED" : "ERROR");

            var updateOperation = TableOperation.merge(entity);

            table.execute(updateOperation);

            this.logger.log(Level.INFO, "[DebtPositionTableService] END completed");
        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            this.logger.log(Level.SEVERE, () -> "[DebtPositionTableService] Error " + e);
        }
    }


    /**
     * @param debtPositions insert in the table a list of {@link DebtPositionEntity}
     */
    public void batchInsert(List<DebtPositionEntity> debtPositions) {
        createEnv();

        try {
            CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                    .createCloudTableClient()
                    .getTableReference(this.tableName);

            TableBatchOperation batchOperation = new TableBatchOperation();
            debtPositions.forEach(batchOperation::insert);

            table.execute(batchOperation);

        } catch (URISyntaxException | StorageException | InvalidKeyException e) {
            logger.log(Level.SEVERE, () -> "[DebtPositionTableService] Error during batch insert " + e.getLocalizedMessage());
        }
    }

    private void createEnv() {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        try {
            azuriteStorageUtil.createTable(tableName);
        } catch (Exception e) {
            this.logger.severe(String.format("[DebtPositionTableService] Problem to create table or queue: %s", e.getMessage()));
        }
    }


}
