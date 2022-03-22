package it.gov.pagopa.canoneunico.service;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Field;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.tomakehurst.wiremock.WireMockServer;

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
    void createDebtPosition400() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    	Logger logger = Logger.getLogger("testlogging");

    	Field host = gpdClient.getClass().getDeclaredField("gpdHost");
    	host.setAccessible(true); // Suppress Java language access checking
    	host.set(gpdClient, "http://localhost:8080");

    	var result = gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build());
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

    	gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build());

    	verify(postRequestedFor(urlEqualTo("/organizations/A/debtpositions")));
    	assertEquals(true, gpdClient.createDebtPosition(logger, "A", PaymentPositionModel.builder().build()));

    	wireMockServer.stop();
    }

    @Test
    void publishDebtPositionError() {
        Logger logger = Logger.getLogger("testlogging");
        var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd");
        assertFalse(result);
    }

    @Test
    void publishDebtPosition400() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    	Logger logger = Logger.getLogger("testlogging");

    	Field host = gpdClient.getClass().getDeclaredField("gpdHost");
    	host.setAccessible(true); // Suppress Java language access checking
    	host.set(gpdClient, "http://localhost:8080");

    	var result = gpdClient.publishDebtPosition(logger, "idPa", "iupd");
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

    	gpdClient.publishDebtPosition(logger, "idPa", "iupd");

    	verify(postRequestedFor(urlEqualTo("/organizations/idPa/debtpositions/iupd/publish")));
    	assertEquals(true, gpdClient.publishDebtPosition(logger, "idPa", "iupd"));

    	wireMockServer.stop();
    }
}
