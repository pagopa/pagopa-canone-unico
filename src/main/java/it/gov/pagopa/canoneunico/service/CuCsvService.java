package it.gov.pagopa.canoneunico.service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.model.PaymentNoticeError;
import it.gov.pagopa.canoneunico.csv.validaton.PaymentNoticeVerifier;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.entity.EcConfigEntity;
import it.gov.pagopa.canoneunico.entity.IuvEntity;
import it.gov.pagopa.canoneunico.entity.Status;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.iuvgenerator.IuvCodeBusiness;
import it.gov.pagopa.canoneunico.model.DebtPositionMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionRowMessage;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.model.RetryStep;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import it.gov.pagopa.canoneunico.util.AzuriteStorageUtil;
import it.gov.pagopa.canoneunico.util.ObjectMapperUtils;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;


@NoArgsConstructor
public class CuCsvService {

    private final String iuvGenerationType = System.getenv("IUV_GENERATION_TYPE");
    private final List<EcConfigEntity> organizationsList = new ArrayList<>();
    private String storageConnectionString = System.getenv("CU_SA_CONNECTION_STRING");
    private String containerInputBlob = System.getenv("INPUT_CSV_BLOB");
    private String containerErrorBlob = System.getenv("ERROR_CSV_BLOB");
    private String debtPositionTable = System.getenv("DEBT_POSITIONS_TABLE");
    private String iuvsTable = System.getenv("IUVS_TABLE");
    private String ecConfigTable = System.getenv("ORGANIZATIONS_CONFIG_TABLE");
    private String debtPositionQueue = System.getenv("DEBT_POSITIONS_QUEUE");
    private Integer segregationCode = NumberUtils.toInt(System.getenv("CU_SEGREGATION_CODE"));
    private Integer batchSizeDebtPosQueue = System.getenv("BATCH_SIZE_DEBT_POS_QUEUE") != null ? NumberUtils.toInt(System.getenv("BATCH_SIZE_DEBT_POS_QUEUE")) : 25;
    private Integer batchSizeDebtPosTable = System.getenv("BATCH_SIZE_DEBT_POS_TABLE") != null ? NumberUtils.toInt(System.getenv("BATCH_SIZE_DEBT_POS_TABLE")) : 25;
    private final String debPositionsIuvPrefix = System.getenv("DEBT_POSITIONS_IUV_PREFIX");
    private Logger logger;


    public CuCsvService(Logger logger) {
        this.logger = logger;
    }

    public CuCsvService(String storageConnectionString, String containerInputBlob,
                        String containerErrorBlob, String debtPositionTable, String debtPositionQueue, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.containerInputBlob = containerInputBlob;
        this.containerErrorBlob = containerErrorBlob;
        this.debtPositionTable = debtPositionTable;
        this.debtPositionQueue = debtPositionQueue;
        this.logger = logger;
    }

    public CuCsvService(String storageConnectionString, String debtPositionTable, String iuvsTable, String segregationCode, Logger logger) {
        this.storageConnectionString = storageConnectionString;
        this.debtPositionTable = debtPositionTable;
        this.iuvsTable = iuvsTable;
        this.segregationCode = NumberUtils.toInt(segregationCode);
        this.logger = logger;
    }

    public CuCsvService(String storageConnectionString, String ecConfigTable, int batchSize, Logger logger) {
        this.batchSizeDebtPosQueue = batchSize;
        this.batchSizeDebtPosTable = batchSize;
        this.storageConnectionString = storageConnectionString;
        this.ecConfigTable = ecConfigTable;
        this.logger = logger;
    }

    public void initEcConfigList() throws URISyntaxException, InvalidKeyException, StorageException, CanoneUnicoException {

        final String ecConfigTablePartitionKey = "org";

        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createTable(ecConfigTable);

        CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                .createCloudTableClient()
                .getTableReference(ecConfigTable);

        // Iterate through the results and add to list
        for (EcConfigEntity entity : table.execute(TableQuery.from(EcConfigEntity.class).where((TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, ecConfigTablePartitionKey))))) {
            organizationsList.add(entity);
        }

        if (organizationsList.isEmpty()) {
            throw new CanoneUnicoException(
                    "[CuCsvService] Init Ec Config Error: unable to retrieve the ecConfig entities for the PartitionKey: " + ecConfigTablePartitionKey);
        }

    }

    public CsvToBean<PaymentNotice> parseCsv(String content) {

        Reader reader = new StringReader(content);
        // Create Mapping Strategy to arrange the column name
        HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy =
                new HeaderColumnNameMappingStrategy<>();
        mappingStrategy.setType(PaymentNotice.class);

        return new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withMappingStrategy(mappingStrategy)
                .withVerifier(new PaymentNoticeVerifier(organizationsList))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();
    }

    public void uploadCsv(String fileName, String content) {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createBlob(containerErrorBlob);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerErrorBlob);
        BlockBlobClient blockBlobClient = cont.getBlobClient(fileName).getBlockBlobClient();
        InputStream stream = new ByteArrayInputStream(content.getBytes());
        blockBlobClient.upload(stream, content.getBytes().length);
    }

    public void deleteCsv(String fileName) {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createBlob(containerInputBlob);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(this.storageConnectionString).buildClient();
        BlobContainerClient cont = blobServiceClient.getBlobContainerClient(containerInputBlob);
        cont.getBlobClient(fileName).delete();
    }

    public List<DebtPositionEntity> saveDebtPosition(String fileName, List<PaymentNotice> payments) throws CanoneUnicoException {
        this.logger.log(Level.INFO, () -> "[CuCsvService] save debt position in table for file " + fileName);

        List<DebtPositionEntity> savedDebtPositionEntities = new ArrayList<>();
        List<List<DebtPositionEntity>> partitionDebtPositionEntities = Lists.partition(this.getDebtPositionEntities(fileName, payments), batchSizeDebtPosTable);

        // save debt positions partition in table
        IntStream.range(0, partitionDebtPositionEntities.size()).forEach(partitionAddIndex -> {
            try {
                List<DebtPositionEntity> partitionBlock = partitionDebtPositionEntities.get(partitionAddIndex);
                this.addDebtPositionEntityList(partitionBlock);
                logger.log(Level.INFO, () -> "[CuCsvService] Azure Table Storage - Add for partition index " + partitionAddIndex + " executed.");
                savedDebtPositionEntities.addAll(partitionBlock);
            } catch (InvalidKeyException | URISyntaxException | StorageException e) {
                logger.log(Level.SEVERE, () -> "[CuCsvService] Exception in add Azure Table Storage batch debt position entities: " + e.getMessage() + " " + e.getCause());
            }
        });

        return savedDebtPositionEntities;
    }

    public boolean pushDebtPosition(String fileName, List<DebtPositionEntity> debtPositionEntities) {

        this.logger.log(Level.INFO, () -> "[CuCsvService] push debt position in queue for file " + fileName);

        AtomicBoolean isAllMsgPushed = new AtomicBoolean(true);

        DebtPositionMessage debtPositionMessage = new DebtPositionMessage();
        debtPositionMessage.setCsvFilename(fileName);
        debtPositionMessage.setRetryCount(0);

        List<List<DebtPositionRowMessage>> msgRows = Lists.partition(this.getDebtPositionQueueMsg(debtPositionEntities), batchSizeDebtPosQueue);

        // push debt positions partition in queue
        IntStream.range(0, msgRows.size()).forEach(partitionAddIndex -> {
            try {
                debtPositionMessage.setRows(msgRows.get(partitionAddIndex));
                this.addDebtPositionMsg(debtPositionMessage);
                logger.log(Level.INFO, () -> "[CuCsvService] Azure Queue Storage - Add for partition index " + partitionAddIndex + " executed.");
            } catch (JsonProcessingException | StorageException | InvalidKeyException | URISyntaxException e) {
                logger.log(Level.SEVERE, () -> "[CuCsvService] Exception in add Azure Queue Storage batch debt position queue msg: " + e.getMessage() + " " + e.getCause());
                isAllMsgPushed.set(false);
            }

        });

        return isAllMsgPushed.get();
    }

    public void addDebtPositionMsg(DebtPositionMessage msg) throws InvalidKeyException, URISyntaxException, StorageException, JsonProcessingException {

        logger.log(Level.INFO, () -> "[CuCsvService] pushing debt position in queue [" + debtPositionQueue + "]: " + msg);

        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createQueue(debtPositionQueue);

        CloudQueue queue = CloudStorageAccount.parse(storageConnectionString).
                createCloudQueueClient()
                .getQueueReference(debtPositionQueue);

        queue.addMessage(new CloudQueueMessage(ObjectMapperUtils.writeValueAsString(msg)));
    }

    public void addDebtPositionEntityList(List<DebtPositionEntity> debtPositionEntities) throws InvalidKeyException, URISyntaxException, StorageException {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createTable(debtPositionTable);

        CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                .createCloudTableClient()
                .getTableReference(debtPositionTable);

        TableBatchOperation batchOperation = new TableBatchOperation();

        debtPositionEntities.forEach(batchOperation::insert);

        table.execute(batchOperation);
    }

    public void checkIUVExistence(IuvEntity iuvEntity) throws InvalidKeyException, URISyntaxException, StorageException {
        AzuriteStorageUtil azuriteStorageUtil = new AzuriteStorageUtil();
        azuriteStorageUtil.createTable(iuvsTable);

        CloudTable table = CloudStorageAccount.parse(storageConnectionString)
                .createCloudTableClient()
                .getTableReference(iuvsTable);

        TableBatchOperation batchOperation = new TableBatchOperation();

        batchOperation.insert(iuvEntity);

        table.execute(batchOperation);
    }


    public String generateErrorCsv(String converted, DebtPositionValidationCsv csvValidationErrors) {

        StringWriter csv = new StringWriter();

        try (Reader reader = new StringReader(converted)) {
            // Create Mapping Strategy to arrange the column name
            HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy =
                    new HeaderColumnNameMappingStrategy<>();
            mappingStrategy.setType(PaymentNotice.class);

            CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                    .withSeparator(';')
                    .withOrderedResults(true)
                    .withMappingStrategy(mappingStrategy)
                    .withType(PaymentNotice.class)
                    .build();

            List<PaymentNoticeError> paymentsToWrite = ObjectMapperUtils.mapAll(csvToBean.parse(), PaymentNoticeError.class);

            for (DebtPositionErrorRow e : csvValidationErrors.getErrorRows()) {
                PaymentNoticeError p = paymentsToWrite.get((int) (e.getRowNumber() - 2));
                p.setErrorsNote("validation error: " + e.getErrorsDetail());
            }


            // Create Mapping Strategy to arrange the column name
            ColumnPositionMappingStrategy<PaymentNoticeError> mappingStrategyWrite =
                    new ColumnPositionMappingStrategy<>();
            mappingStrategyWrite.setType(PaymentNoticeError.class);

            StringWriter writer = new StringWriter();
            StatefulBeanToCsvBuilder<PaymentNoticeError> builder = new StatefulBeanToCsvBuilder<>(writer);
            StatefulBeanToCsv<PaymentNoticeError> beanWriter = builder
                    .withSeparator(';')
                    .withMappingStrategy(mappingStrategyWrite)
                    .withOrderedResults(true)
                    .build();
            beanWriter.write(paymentsToWrite);

            String headers = "id;pa_id_istat;pa_id_catasto;pa_id_fiscal_code;pa_id_cbill;pa_pec_mail;pa_referent_email;pa_referent_name;amount;debtor_id_fiscal_code;"
                    + "debtor_name;debtor_email;payment_notice_number;note;errors_note";
            String footer = "nLinesError/nTotLines:" + csvValidationErrors.getNumberInvalidRows() + "/" + csvValidationErrors.getTotalNumberRows();
            csv.append(headers);
            csv.append(System.lineSeparator());
            csv.append(writer.toString());
            csv.append(System.lineSeparator());
            csv.append(footer);
            csv.flush();
            csv.close();

        } catch (IOException | CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
            logger.log(Level.SEVERE, () -> "[CuCsvService generateErrorCsv] Error " + e.getMessage() + " "
                    + e.getCause());
        }
        return csv.toString();
    }

    public String getValidIUV(String paIdFiscalCode, int segregationCode) throws CanoneUnicoException {
        final int MAX_RETRY_COUNT = 7;
        int retryCount = 1;
        String iuv = null;
        while (true) {
            try {
                if (null != iuvGenerationType && iuvGenerationType.equalsIgnoreCase("seq")) {
                    iuv = this.generateIncrementalIUV(segregationCode, 0);
                } else {
                    iuv = this.generateIUV(segregationCode);
                }
                IuvEntity iuvEntity = new IuvEntity(paIdFiscalCode, iuv);
                this.checkIUVExistence(iuvEntity);
                break;
            } catch (InvalidKeyException | URISyntaxException | StorageException e) {
                if (retryCount > MAX_RETRY_COUNT) {
                    throw new CanoneUnicoException(
                            "[CuCsvService] Azure Table Storage - table [" + iuvsTable + "]: Unable to get a unique IUV in " + MAX_RETRY_COUNT + " retry",
                            e);
                }
                logger.log(Level.WARNING, String.format(
                        "[CuCsvService] Azure Table Storage - Not unique IUV [%s] in table [%s]: a new one will be generated [retry = %s].",
                        iuv, iuvsTable, retryCount));
                retryCount++;
            }
        }
        return iuv;
    }

    public String generateIUV(int segregationCode) {
        return IuvCodeBusiness.generateIUV(segregationCode);
    }

    public String generateIncrementalIUV(int segregationCode, int nextVal) {
        return IuvCodeBusiness.generateIUV(segregationCode, nextVal);
    }

    private List<DebtPositionEntity> getDebtPositionEntities(String fileName, List<PaymentNotice> payments) throws CanoneUnicoException {
        List<DebtPositionEntity> debtPositionEntities = new ArrayList<>();
        for (PaymentNotice p : payments) {
            DebtPositionEntity e = new DebtPositionEntity(fileName, p.getId());
            e.setPaIdIstat(p.getPaIdIstat());
            e.setPaIdCatasto(p.getPaIdCatasto());
            e.setPaIdFiscalCode(p.getPaIdFiscalCode());
            e.setPaIdCbill(p.getPaIdCBill());
            e.setPaPecEmail(p.getPaPecEmail());
            e.setPaReferentName(p.getPaReferentName());
            e.setPaReferentEmail(p.getPaReferentEmail());
            e.setDebtorIdFiscalCode(p.getDebtorFiscalCode());
            e.setNote(p.getNote());
            e.setDebtorName(p.getDebtorName());
            e.setDebtorEmail(p.getDebtorEmail());
            e.setAmount(String.valueOf(p.getAmount()));
            e.setStatus(Status.INSERTED.name());
            // enrich entity with info from ec_config
            this.enrichDebtPositionEntity(e);
            // generate iuv and iupd if status is not SKIPPED
            if (!e.getStatus().equals(Status.SKIPPED.name())) {
                String iuv = this.getValidIUV(e.getPaIdFiscalCode(), segregationCode);
                e.setPaymentNoticeNumber(iuv);
                e.setIupd(this.generateIUPD(iuv));
            }

            debtPositionEntities.add(e);

        }
        return debtPositionEntities;
    }

	private void enrichDebtPositionEntity(DebtPositionEntity e) throws CanoneUnicoException {
		// get data from ec_config
        EcConfigEntity ecConfig = organizationsList.stream()
                .filter(o -> o.getPaIdCatasto().equals(e.getPaIdCatasto()) || o.getPaIdIstat().equals(e.getPaIdIstat()) || o.getRowKey().equals(e.getPaIdFiscalCode()))
                .findFirst()
                .orElseThrow(() -> new CanoneUnicoException("[CuCsvService] Enrich Payment Info Error: unable to retrieve the ecConfig entity for paIdCatasto = " + e.getPaIdCatasto() + " or paIdIstat = " + e.getPaIdIstat() + " or paFiscalCode = " + e.getPaIdFiscalCode()));

		// convention: skip if iban not present
		if (null == ecConfig.getIban() || ecConfig.getIban().isEmpty()) {
            logger.warning("[CuCsvService] Enrich Payment with ecConfig info: EC [idCatasto=" + ecConfig.getPaIdCatasto() + "; idIstat=" + ecConfig.getPaIdIstat() + "; paIdFiscalCode = " + e.getPaIdFiscalCode() + "] is without iban -> set status to SKIPPED");
            // overwrite the state to skipped
			e.setStatus(Status.SKIPPED.name());
			e.setNote(Status.SKIPPED.name());
		}
		e.setPaIdFiscalCode(ecConfig.getRowKey());
		e.setPaIdIstat(ecConfig.getPaIdIstat());
		e.setPaIdCatasto(ecConfig.getPaIdCatasto());
		e.setPaIdCbill(ecConfig.getPaIdCbill());
		e.setPaPecEmail(ecConfig.getPaPecEmail());
		e.setPaReferentEmail(ecConfig.getPaReferentEmail());
		e.setPaReferentName(ecConfig.getPaReferentName());
		e.setCompanyName(ecConfig.getCompanyName());
		e.setIban(ecConfig.getIban());

	}

    private List<DebtPositionRowMessage> getDebtPositionQueueMsg(List<DebtPositionEntity> debtPositionEntities) {
        List<DebtPositionRowMessage> debtPositionMsgs = new ArrayList<>();
        for (DebtPositionEntity e : debtPositionEntities) {
            if (!e.getStatus().equals(Status.SKIPPED.name())) {
                DebtPositionRowMessage row = new DebtPositionRowMessage();
                row.setId(e.getRowKey());
                row.setDebtorName(e.getDebtorName());
                row.setDebtorEmail(e.getDebtorEmail());
                row.setAmount(Long.parseLong(e.getAmount()));
                row.setIuv(e.getPaymentNoticeNumber());
                row.setIupd(e.getIupd());
                row.setPaIdFiscalCode(e.getPaIdFiscalCode());
                row.setDebtorIdFiscalCode(e.getDebtorIdFiscalCode());
                row.setCompanyName(e.getCompanyName());
                row.setIban(e.getIban());
                row.setRetryAction(RetryStep.NONE.name());
                debtPositionMsgs.add(row);
            }
        }
        return debtPositionMsgs;
    }

    private String generateIUPD(String iuv) {
        return debPositionsIuvPrefix + iuv;
    }

}
