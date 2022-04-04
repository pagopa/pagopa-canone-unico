package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.service.DebtPositionQueueService;
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Mock
    DebtPositionQueueService queueService;


    @Test
    void run() throws JsonProcessingException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(gpdClient).when(function).getGpdClientInstance();
        when(gpdClient.createDebtPosition(any(), any(), any(), any())).thenReturn(201);
        when(gpdClient.publishDebtPosition(any(), any(), any(), any())).thenReturn(200);
        doReturn(tableService).when(function).getDebtPositionTableService(logger);

        String message = new ObjectMapper().writeValueAsString(DebtPositionMessage.builder()
                .csvFilename("csv")
                .rows(List.of(DebtPositionRowMessage.builder()
                        .id("001")
                        .amount(100L)
                        .iuv("IUV")
                        .iupd("IUPD")
                        .paIdFiscalCode("PAFISCALCODE")
                        .debtorIdFiscalCode("DEBTORFISCALCODE")
                        .debtorName("DEBTORNAME")
                        .debtorEmail("DEBTOREMAIL")
                        .companyName("COMPANY")
                        .iban("IBAN")
                        .retryAction("NONE")
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(function, times(2)).getGpdClientInstance();
        verify(function, times(1)).getDebtPositionTableService(logger);
        verify(gpdClient, times(1)).createDebtPosition(any(), any(), any(), any());
        verify(gpdClient, times(1)).publishDebtPosition(any(), any(), any(), any());
        verify(tableService, times(1)).updateEntity(anyString(), any(), anyBoolean(), any());

    }

    @Test
    void runFailed() throws JsonProcessingException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(gpdClient).when(function).getGpdClientInstance();
        when(gpdClient.createDebtPosition(any(), any(), any(), any())).thenReturn(201);
        when(gpdClient.publishDebtPosition(any(), any(), any(), any())).thenReturn(200);
        doReturn(tableService).when(function).getDebtPositionTableService(logger);

        String message = new ObjectMapper().writeValueAsString(DebtPositionMessage.builder()
                .csvFilename("csv")
                .retryCount(0)
                .rows(List.of(DebtPositionRowMessage.builder()
                        .id("001")
                        .amount(100L)
                        .iuv("IUV")
                        .iupd("IUPD")
                        .paIdFiscalCode("PAFISCALCODE")
                        .debtorIdFiscalCode("DEBTORFISCALCODE")
                        .debtorName("DEBTORNAME")
                        .debtorEmail("DEBTOREMAIL")
                        .companyName("COMPANY")
                        .iban("IBAN")
                        .retryAction("NONE")
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(function, times(2)).getGpdClientInstance();
        verify(function, times(1)).getDebtPositionTableService(logger);
        verify(gpdClient, times(1)).createDebtPosition(any(), any(), any(), any());
        verify(gpdClient, times(1)).publishDebtPosition(any(), any(), any(), any());
        verify(tableService, times(1)).updateEntity(anyString(), any(), anyBoolean(), any());

    }

    @Test
    void runFailed2() throws JsonProcessingException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(gpdClient).when(function).getGpdClientInstance();
        when(gpdClient.createDebtPosition(any(), any(), any(), any())).thenReturn(201);
        when(gpdClient.publishDebtPosition(any(), any(), any(), any())).thenReturn(500);
        doReturn(queueService).when(function).getDebtPositionQueueService(logger);

        String message = new ObjectMapper().writeValueAsString(DebtPositionMessage.builder()
                .csvFilename("csv")
                .retryCount(-1) // to trigger retry during test
                .rows(List.of(DebtPositionRowMessage.builder()
                        .id("001")
                        .amount(100L)
                        .iuv("IUV")
                        .iupd("IUPD")
                        .paIdFiscalCode("PAFISCALCODE")
                        .debtorIdFiscalCode("DEBTORFISCALCODE")
                        .debtorName("DEBTORNAME")
                        .debtorEmail("DEBTOREMAIL")
                        .companyName("COMPANY")
                        .iban("IBAN")
                        .retryAction("NONE")
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(function, times(2)).getGpdClientInstance();
        verify(function, times(1)).getDebtPositionQueueService(logger);
        verify(gpdClient, times(1)).createDebtPosition(any(), any(), any(), any());
        verify(gpdClient, times(1)).publishDebtPosition(any(), any(), any(), any());
        verify(queueService, times(1)).insertMessage(any());
    }

    @Test
    void runFailed3() throws JsonProcessingException {
        // general var
        Logger logger = Logger.getLogger("testlogging");

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(gpdClient).when(function).getGpdClientInstance();
        when(gpdClient.createDebtPosition(any(), any(), any(), any())).thenReturn(201);
        when(gpdClient.publishDebtPosition(any(), any(), any(), any())).thenReturn(400);
        doReturn(tableService).when(function).getDebtPositionTableService(logger);

        String message = new ObjectMapper().writeValueAsString(DebtPositionMessage.builder()
                .csvFilename("csv")
                .retryCount(-1) // to trigger retry during test
                .rows(List.of(DebtPositionRowMessage.builder()
                        .id("001")
                        .amount(100L)
                        .iuv("IUV")
                        .iupd("IUPD")
                        .paIdFiscalCode("PAFISCALCODE")
                        .debtorIdFiscalCode("DEBTORFISCALCODE")
                        .debtorName("DEBTORNAME")
                        .debtorEmail("DEBTOREMAIL")
                        .companyName("COMPANY")
                        .iban("IBAN")
                        .retryAction("NONE")
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(function, times(2)).getGpdClientInstance();
        verify(function, times(1)).getDebtPositionTableService(logger);
        verify(gpdClient, times(1)).createDebtPosition(any(), any(), any(), any());
        verify(gpdClient, times(1)).publishDebtPosition(any(), any(), any(), any());
        verify(tableService, times(1)).updateEntity(any(), any(), anyBoolean(), any());
    }

    @Test
    void getGpdClientInstance() {
        GpdClient client = function.getGpdClientInstance();
        assertNotNull(client);
    }

    @Test
    void getDebtPositionService() {
        // general var
        Logger logger = Logger.getLogger("testlogging");
        DebtPositionTableService tableService = function.getDebtPositionTableService(logger);
        assertNotNull(tableService);
    }

}
