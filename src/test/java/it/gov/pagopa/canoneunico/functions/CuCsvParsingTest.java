package it.gov.pagopa.canoneunico.functions;

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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.storage.StorageException;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.enums.CSVReaderNullFieldIndicator;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.csv.validaton.CsvValidation;
import it.gov.pagopa.canoneunico.csv.validaton.PaymentNoticeVerifier;
import it.gov.pagopa.canoneunico.entity.DebtPositionEntity;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.service.CuCsvService;

@ExtendWith(MockitoExtension.class)
class CuCsvParsingTest {

    @Mock
    ExecutionContext context;

    @Spy
    CuCsvParsing function;

    @Mock
    CuCsvService cuCsvService;
    
    @Spy
    CuCsvService cuCsvServiceSpy;
    
    

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
    void checkParseFileOKTest() throws IOException, InvalidKeyException, StorageException, URISyntaxException {
    	
    	ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS.csv");
        String data = readFromInputStream(inputStream);
    	
    	Logger logger = Logger.getLogger("testlogging");
    	
    	
    	Reader reader = new StringReader(data);
    	// Create Mapping Strategy to arrange the column name
    	HeaderColumnNameMappingStrategy<PaymentNotice> mappingStrategy=
    			new HeaderColumnNameMappingStrategy<>();
    	mappingStrategy.setType(PaymentNotice.class);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
        		.withSeparator(';')
    			.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
    			.withOrderedResults(true)
    			.withMappingStrategy(mappingStrategy)
    			.withVerifier(new PaymentNoticeVerifier())
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
    			.withVerifier(new PaymentNoticeVerifier())
    			.withType(PaymentNotice.class)
    			.withIgnoreLeadingWhiteSpace(true)
    			.withThrowExceptions(false)
    			.build();
        
    	// precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        when(cuCsvService.parseCsv(data)).thenReturn(csvToBean);
    	    
        byte[] file = data.getBytes();

        function.run(file, "2021-04-21_pagcorp0007_0101108TS.csv", context);
        
        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).parseCsv(data);
        verify(cuCsvService, times(1)).saveDebtPosition("2021-04-21_pagcorp0007_0101108TS.csv", payments);
    }
    
    @Test
    void checkParseFileKOTest() throws IOException, InvalidKeyException, StorageException, URISyntaxException {
    	
    	ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("2021-04-21_pagcorp0007_0101108TS2_KO.csv");
        String data = readFromInputStream(inputStream);
    	
    	Logger logger = Logger.getLogger("testlogging");
        
        Reader reader = new StringReader(data);
        CsvToBean<PaymentNotice> csvToBean = new CsvToBeanBuilder<PaymentNotice>(reader)
		.withSeparator(';')
		.withFieldAsNull(CSVReaderNullFieldIndicator.BOTH)
		.withOrderedResults(true)
		.withVerifier(new PaymentNoticeVerifier())
		.withType(PaymentNotice.class)
		.withIgnoreLeadingWhiteSpace(true)
		.withThrowExceptions(false)
		.build();
    	
    	 
    	// precondition
        when(context.getLogger()).thenReturn(logger);
        doReturn(cuCsvService).when(function).getCuCsvServiceInstance(logger);
        when(cuCsvService.parseCsv(data)).thenReturn(csvToBean);
    	
        
        byte[] file = data.getBytes();

        function.run(file, "2021-04-21_pagcorp0007_0101108TS2_KO.csv", context);
        
        verify(context, times(1)).getLogger();
        verify(cuCsvService, times(1)).parseCsv(data);
        verify(cuCsvService, times(1)).uploadCsv("2021-04-21_pagcorp0007_0101108TS2_KO.csv",null);
        verify(cuCsvService, times(1)).deleteCsv("2021-04-21_pagcorp0007_0101108TS2_KO.csv");

    }
}
