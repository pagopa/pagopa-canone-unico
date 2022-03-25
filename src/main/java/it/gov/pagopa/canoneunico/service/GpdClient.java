package it.gov.pagopa.canoneunico.service;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.microsoft.azure.functions.HttpStatus;
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

    public boolean createDebtPosition(Logger logger, String idPa, PaymentPositionModel body) {
        try {
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - createDebtPosition] Calling GPD service: " + idPa);
            Client client = ClientBuilder.newClient();
            Response response = client
                    .register(JacksonJaxbJsonProvider.class)
                    .target(gpdHost + String.format(POST_DEBT_POSITIONS, idPa))
                    .request()
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.json(body));
            client.close();
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - createDebtPosition] HTTP status: " + response.getStatus());
            return response.getStatus() == HttpStatus.CREATED.value();
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR - createDebtPosition] error during the GPD call " + e.getMessage() + " "
                    + e.getCause());
            return false;
        }
    }

    public boolean publishDebtPosition(Logger logger, String idPa, String iupd) {
        try {
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - publishDebtPosition] Calling GPD service: " + idPa +"; "+iupd);
            Client client = ClientBuilder.newClient();
            Response response = client
                    .register(JacksonJaxbJsonProvider.class)
                    .target(gpdHost + String.format(PUBLISH_DEBT_POSITIONS, idPa, iupd))
                    .request()
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.json(null));
            client.close();
            logger.log(Level.INFO, () -> "[CuCreateDebtPositionFunction GPD - publishDebtPosition] HTTP status: " + response.getStatus());
            return response.getStatus() == HttpStatus.OK.value();
        } catch (Exception e) {
            logger.log(Level.SEVERE, () -> "[CuCreateDebtPositionFunction ERROR - publishDebtPosition] error during the GPD call " + e.getMessage() + " "
                    + e.getCause());
            return false;
        }
    }
}
