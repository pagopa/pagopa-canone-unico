package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import com.microsoft.azure.storage.StorageException;
import it.gov.pagopa.canoneunico.service.DebtPositionService;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with Timer trigger.
 */
public class CuGenerateOutputCsv {

    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String debtPositionsTable = System.getenv("DEBT_POSITIONS_TABLE");
    private String containerBlobIn = System.getenv("INPUT_CSV_BLOB");
    private String containerBlobOut = System.getenv("OUTPUT_CSV_BLOB");

    /**
     * This function will be invoked periodically according to the specified
     * schedule.
     */
    //  schedule = "*/5 * * * * *"

    @FunctionName("CuGenerateOutputCsvBatchFunction")
    public void run(
            @TimerTrigger(name = "CuGenerateOutputCsvBatchTrigger", schedule = "%NCRON_SCHEDULE_BATCH%") String timerInfo,
            final ExecutionContext context
    ) {

        Logger logger = context.getLogger();

        logger.log(Level.INFO, () -> "[CuGenerateOutputCsvBatchFunction] function executed at: " + LocalDateTime.now());

        DebtPositionService debtPositionService = this.getDebtPositionServiceInstance(logger);

        List<String> csvFileNamePks = debtPositionService.getCsvFilePk();

        csvFileNamePks.forEach(blob -> logger.log(Level.INFO, () ->
                "[CuGenerateOutputCsvBatchFunction] cvs file name  " + blob));

        List<List<String>> data = null;
        try {
            data = debtPositionService.getDebtPositionListByPk(csvFileNamePks);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        }


        data.forEach(datar -> logger.log(Level.INFO, () ->
                "[CuGenerateOutputCsvBatchFunction] row data  " + datar));

    }

    public DebtPositionService getDebtPositionServiceInstance(Logger logger) {
        return new DebtPositionService(this.storageConnectionString, this.debtPositionsTable, this.containerBlobIn, this.containerBlobOut, logger);
    }

}
