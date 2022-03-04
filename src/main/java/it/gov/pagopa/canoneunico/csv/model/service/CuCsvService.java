package it.gov.pagopa.canoneunico.csv.model.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.logging.Logger;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;



public class CuCsvService {

    private String storageConnectionString;
    private Logger logger;

    public CuCsvService(String storageConnectionString, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.logger = logger;
    }
    
    public void create(String containerBlob, String fileName, String content) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerBlob);
		BlockBlobClient blockBlobClient = cont.getBlobClient(fileName).getBlockBlobClient();
		InputStream stream = new ByteArrayInputStream(content.getBytes());
		blockBlobClient.upload(stream, content.getBytes().length);
    }

    public void delete(String containerBlob, String fileName) {
    	BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
		BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerBlob);
		cont.getBlobClient(fileName).delete();
    }
    
}
