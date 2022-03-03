package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.time.LocalDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with Timer trigger.
 */
public class CuGenerateOutputCsv {

    private String storageConnectionString = System.getenv("FLOW_SA_CONNECTION_STRING");

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


    }

}
