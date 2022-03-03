package it.gov.pagopa.canoneunico.service;

import com.microsoft.azure.functions.HttpStatus;
import it.gov.pagopa.canoneunico.model.PaymentPositionModel;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GpdClient {

    private static final String POST_DEBT_POSITIONS = "/organizations/%s/debtpositions";
    private static GpdClient instance = null;
    private final String gpdHost = System.getenv("GPD_HOST");

    public static GpdClient getInstance() {
        if (instance == null) {
            instance = new GpdClient();
        }
        return instance;
    }

    public boolean createDebtPosition(String idPa, PaymentPositionModel body) {
        try {
            Response response = ClientBuilder.newClient()
                    .target(gpdHost + String.format(POST_DEBT_POSITIONS, idPa))
                    .request()
                    .accept(MediaType.APPLICATION_JSON)
                    .post(Entity.json(body));
            return response.getStatus() == HttpStatus.CREATED.value();
        } catch (Exception e) {
            return false;
        }
    }
}
