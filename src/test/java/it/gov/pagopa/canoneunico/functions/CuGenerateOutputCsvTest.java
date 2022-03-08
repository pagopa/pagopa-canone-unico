package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.storage.StorageException;
import it.gov.pagopa.canoneunico.service.DebtPositionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CuGenerateOutputCsvTest {

    @Spy
    CuGenerateOutputCsv function;

    @Mock
    ExecutionContext context;

    @Mock
    DebtPositionService debtPositionService;


    @Test
    void run() throws URISyntaxException, InvalidKeyException, StorageException, FileNotFoundException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(debtPositionService).when(function).getDebtPositionServiceInstance(logger);

        function.run("CuGenerateOutputCsvBatchTrigger", context);

        // Asserts
        verify(context, times(1)).getLogger();
        verify(debtPositionService, times(1)).getDebtPositionListByPk(any());
        verify(debtPositionService, times(0)).uploadOutFile(anyString(), any());

    }

}
