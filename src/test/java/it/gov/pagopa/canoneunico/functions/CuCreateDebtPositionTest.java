package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;

import it.gov.pagopa.canoneunico.functions.CuCreateDebtPosition;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.service.DebtPositionTableService;
import it.gov.pagopa.canoneunico.service.GpdClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CuCreateDebtPositionTest {

    @Spy
    CuCreateDebtPosition function;

    @Mock
    ExecutionContext context;

    @Mock
    GpdClient gpdClient;

    @Mock
    DebtPositionTableService tableService;


    @Test
    void run() throws JsonProcessingException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(gpdClient).when(function).getGpdClientInstance();
        doReturn(tableService).when(function).getDebtPositionService(logger);

        String message = new ObjectMapper().writeValueAsString(DebtPositionMessage.builder()
                .csvFilename("csv")
                .rows(List.of(DebtPositionRowMessage.builder()
                        .amount(100L)
                        .fiscalCode("A")
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(gpdClient, times(1)).createDebtPosition(any(), any());
        verify(tableService, times(1)).updateEntity(anyString(), any(), anyBoolean());

    }

}
