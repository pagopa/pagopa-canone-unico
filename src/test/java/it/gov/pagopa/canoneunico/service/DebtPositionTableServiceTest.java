package it.gov.pagopa.canoneunico.service;

import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

@Testcontainers
class DebtPositionTableServiceTest {

    @ClassRule
    @Container
    public static GenericContainer<?> azurite = new GenericContainer<>(
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest")).withExposedPorts(10001, 10002,
            10000);


    String storageConnectionString = String.format(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://%s:%s/devstoreaccount1;QueueEndpoint=http://%s:%s/devstoreaccount1",
            azurite.getContainerIpAddress(), azurite.getMappedPort(10002), azurite.getContainerIpAddress(),
            azurite.getMappedPort(10001));


    @Test
    void updateEntity() {
        Logger logger = Logger.getLogger("testlogging");

        var tableService = spy(new DebtPositionTableService(storageConnectionString, "table", true, logger));
        DebtPositionEntity entity = new DebtPositionEntity("csv", "1");
        entity.setStatus(Status.INSERTED.name());
        tableService.batchInsert(List.of(entity));
        tableService.updateEntity("csv", DebtPositionRowMessage.builder()
                        .id("1")
                        .paIdFiscalCode("PAFISCALCODE")
                        .debtorIdFiscalCode("DEBTORFISCALCODE")
                        .build(),
                true);
        var res = tableService.getEntity("csv", "1");
        assertEquals(Status.CREATED.name(), res.getStatus());
    }
}
