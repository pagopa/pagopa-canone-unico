package it.gov.pagopa.canoneunico.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import it.gov.pagopa.canoneunico.model.PaymentPositionModel;

@ExtendWith(MockitoExtension.class)
class GpdClientTest {

    @Spy
    GpdClient gpdClient;
    
    @Mock
    private Client client = ClientBuilder.newClient();

    @Test
    void getInstance() {
        var result = GpdClient.getInstance();
        assertNotNull(result);
    }
    
    @Test
    void createDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build());
        assertFalse(result);
    }
    
    @Test
    void createDebtPosition400() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    	Logger logger = Logger.getLogger("testlogging");
    	
    	Field host = gpdClient.getClass().getDeclaredField("gpdHost");
    	host.setAccessible(true); // Suppress Java language access checking
    	host.set(gpdClient, "http://localhost:8080/");
    	
    	var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build());
    	assertFalse(result);
    }
    
    @Test
    void publishDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd");
        assertFalse(result);
    }
}
