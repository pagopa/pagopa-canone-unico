package it.gov.pagopa.canoneunico.functions;

import com.azure.storage.blob.models.BlobStorageException;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import com.microsoft.azure.storage.StorageException;
import it.gov.pagopa.canoneunico.model.CsvOutModel;
import it.gov.pagopa.canoneunico.service.DebtPositionService;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Azure Functions with Timer trigger.
 */
public class CuGenerateOutputCsv {

    private final String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private final String debtPositionsTable = System.getenv("DEBT_POSITIONS_TABLE");

    /**
     * This function will be invoked periodically according to the specified schedule.
     */

    @FunctionName("CuGenerateOutputCsvBatchFunction")
    public void run(
            @TimerTrigger(name = "CuGenerateOutputCsvBatchTrigger", schedule = "%NCRON_SCHEDULE_BATCH%")
                    String timerInfo,
            final ExecutionContext context) {

        Logger logger = context.getLogger();

        logger.log(
                Level.INFO,
                () -> "[CuGenerateOutputCsvBatchFunction] function executed at: " + LocalDateTime.now());

        DebtPositionService debtPositionService = this.getDebtPositionServiceInstance(logger);

        List<String> csvFileNamePks = debtPositionService.getCsvFilePk();

        List<List<List<String>>> data;
        try {
            data = debtPositionService.getDebtPositionListByPk(csvFileNamePks);

            List<List<List<String>>> finalData = data;
            List<CsvOutModel> csvOut =
                    IntStream.range(0, csvFileNamePks.size())
                            .mapToObj(i -> AzuriteStorageUtil.getOutByBlobKey(csvFileNamePks.get(i), finalData.get(i)))
                            .collect(Collectors.toList());

            csvOut.forEach(out -> {
                        logger.log(Level.INFO, () -> "[CuGenerateOutputCsvBatchFunction][" + out.getContainerName() + "][" + out.getCsvFileName() + "] try to generate output in InputStorage - rows number " + out.getData().size());
                        try {
                            if (!out.getData().isEmpty()) {
                                debtPositionService.uploadOutFile(out.getContainerName(), out.getCsvFileName(), out.getData());
                            }
                        } catch (BlobStorageException | IOException ex) {
                            logger.log(Level.SEVERE, () -> "[CuGenerateOutputCsvBatchFunction][" + out.getCsvFileName() + "] error: " + ex.getLocalizedMessage());
                        }
                    });
        } catch (URISyntaxException | InvalidKeyException | StorageException e) {
            logger.log(Level.SEVERE, () -> "[CuGenerateOutputCsvBatchFunction] error: " + e.getLocalizedMessage());
        }
    }

    public DebtPositionService getDebtPositionServiceInstance(Logger logger) {
        return new DebtPositionService(
                this.storageConnectionString,
                this.debtPositionsTable,
                logger);
    }
}
