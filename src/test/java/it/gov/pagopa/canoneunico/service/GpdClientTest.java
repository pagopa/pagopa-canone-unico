package it.gov.pagopa.canoneunico.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import it.gov.pagopa.canoneunico.model.PaymentPositionModel;

@ExtendWith(MockitoExtension.class)
class GpdClientTest {

    @Spy
    GpdClient gpdClient;

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
    void publishDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd");
        assertFalse(result);
    }
}
