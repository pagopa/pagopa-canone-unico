package it.gov.pagopa.canoneunico.functions;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Logger;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;

import it.gov.pagopa.canoneunico.csv.model.service.CuCsvService;

@ExtendWith(MockitoExtension.class)
class CuCsvParsingTest {
	
	/*
	@ClassRule
    @Container
    public static GenericContainer<?> azurite = new GenericContainer<>(
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest")).withExposedPorts(10001, 10002,
                    10000);
	
	String storageConnectionString = String.format(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://%s:%s/devstoreaccount1;QueueEndpoint=http://%s:%s/devstoreaccount1",
            azurite.getContainerIpAddress(), azurite.getMappedPort(10002), azurite.getContainerIpAddress(),
            azurite.getMappedPort(10001));*/
	
	String storageConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1";
	
	String containerInputBlob = "input";
    String containerErrorBlob = "error";

    @Mock
    ExecutionContext context;

    @Spy
    CuCsvParsing function;

    @Mock
    CuCsvService cuCsvService;

    private String readFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultStringBuilder.toString();
    }

    @Test
    void runOkTest() throws IOException, InvalidKeyException, StorageException, URISyntaxException {
    	
    	//CloudStorageAccount.parse(storageConnectionString).createCloudBlobClient().getContainerReference(this.containerInputBlob).createIfNotExists();

    	//CloudStorageAccount.parse(storageConnectionString).createCloudBlobClient().getContainerReference(this.containerErrorBlob).createIfNotExists();


        when(context.getLogger()).thenReturn(Logger.getLogger("InfoLogging"));

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS.csv");
        String data = readFromInputStream(inputStream);

        byte[] file = data.getBytes();

        Logger logger = Logger.getLogger("InfoLogging");

        function.run(file, "2021-04-21_pagcorp0007_0101108TS.csv", context);

        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).parseCsv(data);
        //verify(cuCsvService, times(1)).uploadCsv(containerErrorBlob,"2021-04-21_pagcorp0007_0101108TS.csv",data);
        //verify(cuCsvService, times(1)).deleteCsv(containerInputBlob,data);

    }
}
