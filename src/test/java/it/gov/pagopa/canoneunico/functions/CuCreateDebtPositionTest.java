package it.gov.pagopa.canoneunico.functions;

import com.microsoft.azure.functions.ExecutionContext;
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

import static org.junit.Assert.assertNotNull;
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
        when(gpdClient.createDebtPosition(any(), any(), any())).thenReturn(true);
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
                        .build()))
                .build());
        function.run(message, context);

        // Asserts
        verify(function, times(2)).getGpdClientInstance();
        verify(function, times(1)).getDebtPositionTableService(logger);
        verify(gpdClient, times(1)).createDebtPosition(any(), any(), any());
        verify(gpdClient, times(1)).publishDebtPosition(any(), any(), any());
        verify(tableService, times(1)).updateEntity(anyString(), any(), anyBoolean());

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
