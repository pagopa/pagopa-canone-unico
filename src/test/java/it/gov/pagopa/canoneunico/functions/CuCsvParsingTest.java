package it.gov.pagopa.canoneunico.functions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.azure.core.util.BinaryData;
import it.gov.pagopa.canoneunico.model.BlobInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.storage.StorageException;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.enums.CSVReaderNullFieldIndicator;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.PaymentNoticeVerifier;
import it.gov.pagopa.canoneunico.entity.EcConfigEntity;
import it.gov.pagopa.canoneunico.exception.CanoneUnicoException;
import it.gov.pagopa.canoneunico.service.CuCsvService;

@ExtendWith(MockitoExtension.class)
class CuCsvParsingTest {

    @Mock
    ExecutionContext context;

    @Spy
    CuCsvParsing function;

    @Mock
    CuCsvService cuCsvService;
    

    private String readFromInputStream(InputStream inputStream) {
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
    void checkParseFileOKTest() throws InvalidKeyException, StorageException, URISyntaxException, CanoneUnicoException {

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS.csv");
        String data = readFromInputStream(inputStream);

        Logger logger = Logger.getLogger("testlogging");
        
        // mock ecConfig organizations list
        List<EcConfigEntity> organizationsList = new ArrayList<>();
        EcConfigEntity ec = new EcConfigEntity("paFiscalCode");
        ec.setPaIdCatasto("C123");
        ec.setCompanyName("company");
        ec.setIban("iban");
        organizationsList.add(ec);


        Reader reader = new StringReader(data);
        // Create Mapping Strategy to arrange the column name
        HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy =
                new HeaderColumnNameMappingStrategy<>();
        mappingStrategy.setType(PaymentNotice.class);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withMappingStrategy(mappingStrategy)
                .withVerifier(new PaymentNoticeVerifier(organizationsList))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();
        List<PaymentNotice> payments = csvToBean.parse();
        reader = new StringReader(data);
        csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withMappingStrategy(mappingStrategy)
                .withVerifier(new PaymentNoticeVerifier(organizationsList))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();

        byte[] file = data.getBytes();
        BlobInfo blobInfo = BlobInfo.builder()
                                    .container("corp")
                                    .name("2021-04-21_pagcorp0007_0101108TS.csv").build();

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        doReturn(BinaryData.fromBytes(file)).when(function).getContent(context, blobInfo);
        doReturn(blobInfo).when(function).getDataFromEvent(context, "events");
        when(cuCsvService.parseCsvToBean(data)).thenReturn(csvToBean);

        function.run("events", context);

        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).initEcConfigList();
        verify(cuCsvService, times(1)).parseCsvToBean(data);
        verify(cuCsvService, times(1)).saveDebtPosition("corp_2021-04-21_pagcorp0007_0101108TS.csv", payments);
    }
    
    @Test
    void checkParseFileOKTest_noIbanValueInEcConfig() throws InvalidKeyException, StorageException, URISyntaxException, CanoneUnicoException {

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS.csv");
        String data = readFromInputStream(inputStream);

        Logger logger = Logger.getLogger("testlogging");
        
        // mock ecConfig organizations list
        List<EcConfigEntity> organizationsList = new ArrayList<>();
        EcConfigEntity ec = new EcConfigEntity("paFiscalCode");
        ec.setPaIdCatasto("C123");
        ec.setCompanyName("company");
        organizationsList.add(ec);


        Reader reader = new StringReader(data);
        // Create Mapping Strategy to arrange the column name
        HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy =
                new HeaderColumnNameMappingStrategy<>();
        mappingStrategy.setType(PaymentNotice.class);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withMappingStrategy(mappingStrategy)
                .withVerifier(new PaymentNoticeVerifier(organizationsList))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();
        List<PaymentNotice> payments = csvToBean.parse();
        reader = new StringReader(data);
        csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withMappingStrategy(mappingStrategy)
                .withVerifier(new PaymentNoticeVerifier(organizationsList))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();

        byte[] file = data.getBytes();
        BlobInfo blobInfo = BlobInfo.builder()
                                    .container("corp")
                                    .name("2021-04-21_pagcorp0007_0101108TS.csv").build();

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        doReturn(BinaryData.fromBytes(file)).when(function).getContent(context, blobInfo);
        doReturn(blobInfo).when(function).getDataFromEvent(context, "events");
        when(cuCsvService.parseCsvToBean(data)).thenReturn(csvToBean);

        function.run("events", context);

        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).initEcConfigList();
        verify(cuCsvService, times(1)).parseCsvToBean(data);
        verify(cuCsvService, times(1)).saveDebtPosition("corp_2021-04-21_pagcorp0007_0101108TS.csv", payments);
    }

    @Test
    void checkParseFileKOTest() throws InvalidKeyException, StorageException, URISyntaxException, CanoneUnicoException {

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS2_KO.csv");
        String data = readFromInputStream(inputStream);

        Logger logger = Logger.getLogger("testlogging");

        Reader reader = new StringReader(data);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withVerifier(new PaymentNoticeVerifier(new ArrayList<>()))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();

        byte[] file = data.getBytes();
        BlobInfo blobInfo = BlobInfo.builder()
                                    .container("corp")
                                    .name("2021-04-21_pagcorp0007_0101108TS.csv").build();

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        doReturn(BinaryData.fromBytes(file)).when(function).getContent(context, blobInfo);
        doReturn(blobInfo).when(function).getDataFromEvent(context, "events");
        when(cuCsvService.parseCsvToBean(data)).thenReturn(csvToBean);

        function.run("events", context);

        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).initEcConfigList();
        verify(cuCsvService, times(1)).parseCsvToBean(data);
        verify(cuCsvService, times(1)).uploadErrorCsv(any(), any(), any());
        verify(cuCsvService, times(1)).deleteCsv(any(), any());
    }
    
    @Test
    void checkParseFileKOTest_noRecordInECConfig() throws InvalidKeyException, StorageException, URISyntaxException, CanoneUnicoException {

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS2_noEC_KO.csv");
        String data = readFromInputStream(inputStream);

        Logger logger = Logger.getLogger("testlogging");

        Reader reader = new StringReader(data);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
                .withOrderedResults(true)
                .withVerifier(new PaymentNoticeVerifier(new ArrayList<>()))
                .withType(PaymentNotice.class)
                .withIgnoreLeadingWhiteSpace(true)
                .withThrowExceptions(false)
                .build();

        byte[] file = data.getBytes();
        BlobInfo blobInfo = BlobInfo.builder()
                                    .container("corp")
                                    .name("2021-04-21_pagcorp0007_0101108TS.csv").build();

        // precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        doReturn(BinaryData.fromBytes(file)).when(function).getContent(context, blobInfo);
        doReturn(blobInfo).when(function).getDataFromEvent(context, "events");
        when(cuCsvService.parseCsvToBean(data)).thenReturn(csvToBean);

        function.run("events", context);

        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).initEcConfigList();
        verify(cuCsvService, times(1)).parseCsvToBean(data);
        verify(cuCsvService, times(1)).uploadErrorCsv(any(), any(), any());
        verify(cuCsvService, times(1)).deleteCsv(any(), any());

    }
}
