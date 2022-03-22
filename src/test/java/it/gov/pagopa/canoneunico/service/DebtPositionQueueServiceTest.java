package it.gov.pagopa.canoneunico.service;

import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.logging.Logger;

import static org.mockito.Mockito.spy;

@Testcontainers
class DebtPositionQueueServiceTest {

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
    void insertMessage() {
        Logger logger = Logger.getLogger("testlogging");

        var queueService = spy(new DebtPositionQueueService(storageConnectionString, "queue", true, logger));
        DebtPositionMessage row = DebtPositionMessage.builder()
                .csvFilename("filename.csv")
                .retryCount(0)
                .rows(List.of(DebtPositionRowMessage.builder()
                        .id("1")
                        .build()))
                .build();
        queueService.insertMessage(row);
    }
}
