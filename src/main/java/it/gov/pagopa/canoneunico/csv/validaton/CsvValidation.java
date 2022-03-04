package it.gov.pagopa.canoneunico.csv.validaton;

import java.util.List;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CsvValidation {

    public static DebtPositionValidationCsvError checkCsvIsValid(String fileName, CsvToBean<PaymentNotice> csvToBean){
    	DebtPositionValidationCsvError debtPosValidationErr = new DebtPositionValidationCsvError();
    	debtPosValidationErr.setCsvFilename(fileName);
    	CsvValidation.checkConstraintErrors(debtPosValidationErr, csvToBean);
        return debtPosValidationErr;
    }
    
    private static void checkConstraintErrors (DebtPositionValidationCsvError debtPosValidationErr, CsvToBean<PaymentNotice> csvToBean) {
    	final List<PaymentNotice> payments = csvToBean.parse();
    	List<CsvException> parsingExceptions = csvToBean.getCapturedExceptions();
    	debtPosValidationErr.setTotalNumberRows(payments.size()+parsingExceptions.size());
    	debtPosValidationErr.setNumberInvalidRows(parsingExceptions.size());
    	if (!parsingExceptions.isEmpty()) {
    		for (CsvException ex: parsingExceptions) {

    			DebtPositionErrorRow errorRow = debtPosValidationErr.getErrorRows().stream().filter(e -> e.getRowNumber() == ex.getLineNumber()).findFirst().orElse(null);
    			if(null != errorRow) {
    				// already exist an error for the row number, add the new one
    				errorRow.getErrorsDetail().add(ex.getMessage());
    			}
    			else {
    				errorRow = new DebtPositionErrorRow();
    				errorRow.setRowNumber(ex.getLineNumber());
    				errorRow.getErrorsDetail().add(ex.getMessage());
    				debtPosValidationErr.getErrorRows().add(errorRow);
    			}	
    		}
    	}

    } 
    
}
