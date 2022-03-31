package it.gov.pagopa.canoneunico.service;

import com.github.tomakehurst.wiremock.WireMockServer;
import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.logging.Logger;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


@ExtendWith(MockitoExtension.class)
class GpdClientTest {

    @Spy
    GpdClient gpdClient;
    private final String requestId = "111";

    @Test
    void getInstance() {
        var result = GpdClient.getInstance();
        Assertions.assertNotNull(result);
    }

    @Test
    void createDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build(), requestId);
        assertFalse(result);
    }

    @Test
    void createDebtPosition400() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Logger logger = Logger.getLogger("testlogging");

        Field host = gpdClient.getClass().getDeclaredField("gpdHost");
        host.setAccessible(true); // Suppress Java language access checking
        host.set(gpdClient, "http://localhost:8080");

        var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build(), requestId);
        assertFalse(result);
    }

    @Test
    void createDebtPosition200() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Logger logger = Logger.getLogger("testlogging");

        WireMockServer wireMockServer = new WireMockServer(8881);
        wireMockServer.start();


        configureFor(wireMockServer.port());
        stubFor(post(urlEqualTo("/organizations/A/debtpositions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")));

        Field host = gpdClient.getClass().getDeclaredField("gpdHost");
        host.setAccessible(true); // Suppress Java language access checking
        host.set(gpdClient, "http://localhost:8881");

        gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build(), requestId);

        verify(postRequestedFor(urlEqualTo("/organizations/A/debtpositions")));
        assertEquals(true, gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build(), requestId));

        wireMockServer.stop();
    }

    @Test
    void publishDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd", requestId);
        assertFalse(result);
    }

    @Test
    void publishDebtPosition400() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Logger logger = Logger.getLogger("testlogging");

        Field host = gpdClient.getClass().getDeclaredField("gpdHost");
        host.setAccessible(true); // Suppress Java language access checking
        host.set(gpdClient, "http://localhost:8080");

        var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd", requestId);
        assertFalse(result);
    }

    @Test
    void publishDebtPosition200() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        Logger logger = Logger.getLogger("testlogging");

        WireMockServer wireMockServer = new WireMockServer(8882);
        wireMockServer.start();


        configureFor(wireMockServer.port());
        stubFor(post(urlEqualTo("/organizations/idPa/debtpositions/iupd/publish")).willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")));

        Field host = gpdClient.getClass().getDeclaredField("gpdHost");
        host.setAccessible(true); // Suppress Java language access checking
        host.set(gpdClient, "http://localhost:8882");

        gpdClient.publishDebtPosition(logger, "idPa", "iupd", requestId);

        verify(postRequestedFor(urlEqualTo("/organizations/idPa/debtpositions/iupd/publish")));
        assertEquals(true, gpdClient.publishDebtPosition(logger, "idPa", "iupd", requestId));

        wireMockServer.stop();
    }
}
