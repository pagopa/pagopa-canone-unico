package it.gov.pagopa.canoneunico.functions;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class CuCsvParsing {
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");

    /**
     * This function will be invoked when a new or updated blob is detected at the
     * specified path. The blob contents are provided as input to this function.
     *
     */
    @FunctionName("CuCsvParsingFunction")
    public void run(
            @BlobTrigger(name = "BlobCsvTrigger", path = "%INPUT_CSV_BLOB%/{name}", dataType = "binary", connection = "CU_SA_CONNECTION_STRING") byte[] content,
            @BindingName("name") String fileName, final ExecutionContext context) {

        // CSV_BLOB = input
        Logger logger = context.getLogger();

        logger.log(Level.INFO, () -> "Blob Trigger function executed at: " + LocalDateTime.now() + " for blob " + fileName);

        // CSV File
        String converted = new String(content, StandardCharsets.UTF_8);

        logger.log(Level.INFO, () -> converted);

    }

}
