package it.gov.pagopa.canoneunico.service;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GpdClient {

    private static final String POST_DEBT_POSITIONS = "/organizations/%s/debtpositions";
    private static final String PUBLISH_DEBT_POSITIONS = "/organizations/%s/debtpositions/%s/publish";
    private static GpdClient instance = null;
    private final String gpdHost = System.getenv("GPD_HOST");

    public static GpdClient getInstance() {
        if (instance == null) {
            instance = new GpdClient();
        }
        return instance;
    }

    public int createDebtPosition(Logger logger, String idPa, PaymentPositionModel body, String requestId) {
        try {
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - createDebtPosition][requestId=" + requestId + "]  Calling GPD service: " + idPa);
            Client client = ClientBuilder.newClient();
            Response response = client
                    .register(JacksonJaxbJsonProvider.class)
                    .target(gpdHost + String.format(POST_DEBT_POSITIONS, idPa))
                    .request()
                    .header("X-Request-Id", requestId)
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.json(body));
            client.close();
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - createDebtPosition][requestId=" + requestId + "] HTTP status: " + response.getStatus());
            return response.getStatus();
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR - createDebtPosition][requestId=" + requestId + "] error during the GPD call " + e.getMessage() + " "
                    + e.getCause());
            return -1;
        }
    }

    public int publishDebtPosition(Logger logger, String idPa, String iupd, String requestId) {
        try {
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - publishDebtPosition][requestId=" + requestId + "] Calling GPD service: " + idPa + "; " + iupd);
            Client client = ClientBuilder.newClient();
            Response response = client
                    .register(JacksonJaxbJsonProvider.class)
                    .target(gpdHost + String.format(PUBLISH_DEBT_POSITIONS, idPa, iupd))
                    .request()
                    .header("X-Request-Id", requestId)
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.json(null));
            client.close();
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - publishDebtPosition][requestId=" + requestId + "] HTTP status: " + response.getStatus());
            return response.getStatus();
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR - publishDebtPosition][requestId=" + requestId + "] error during the GPD call " + e.getMessage() + " "
                    + e.getCause());
            return -1;
        }
    }
}
