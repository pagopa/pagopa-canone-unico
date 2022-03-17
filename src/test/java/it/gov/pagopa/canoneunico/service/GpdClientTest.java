package it.gov.pagopa.canoneunico.service;

import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
class GpdClientTest {

    @Spy
    GpdClient gpdClient;


    @Test
    void createDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build());
        assertFalse(result);
    }
}
