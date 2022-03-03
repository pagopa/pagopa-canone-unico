package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with Azure Queue trigger.
 */
public class CuCreateDebtPosition {

    /**
     * This function will be invoked when a new message is detected in the queue
     */
    @FunctionName("CuCreateDebtPositionFunction")
    public void run(
            @QueueTrigger(name = "DebtPositionTrigger", queueName = "%DEBT_POSITIONS_QUEUE%", connection = "CU_SA_CONNECTION_STRING") String message,
            final ExecutionContext context) {

        Logger logger = context.getLogger();

        try {

            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction START]  processed a message " + message);

            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction END]  processed a message " + message);
        } catch (Exception e) {

            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction Error] Generic Error " + e.getMessage() + " "
                    + e.getCause() + " - message " + message);
        }

    }

}
