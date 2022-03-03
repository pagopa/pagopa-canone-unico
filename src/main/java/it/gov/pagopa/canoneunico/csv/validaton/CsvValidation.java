package it.gov.pagopa.canoneunico.csv.validaton;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsvError;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CsvValidation {

    public static DebtPositionValidationCsvError checkCsvIsValid(String fileName, CsvToBean<?> csvToBean, List<PaymentNotice> payments){
    	DebtPositionValidationCsvError debtPosValidationErr = new DebtPositionValidationCsvError();
    	debtPosValidationErr.setCsvFilename(fileName);
    	debtPosValidationErr.setTotalNumberRows(payments.size());
        checkRequired(debtPosValidationErr, csvToBean);
        checkUniqueId(debtPosValidationErr, payments);
        return debtPosValidationErr;
    }
    
    private static void checkRequired(DebtPositionValidationCsvError debtPosValidationErr, CsvToBean<?> csvToBean) {
    	List<CsvException> parsingExceptions = csvToBean.getCapturedExceptions();
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
    
    private static void checkUniqueId(DebtPositionValidationCsvError debtPosValidationErr, List<PaymentNotice> payments) {
    	final Set<String> unique = new HashSet<>(); 
    	for (int i = 0; i < payments.size(); i++) {
    		String id = payments.get(i).getId();
    		if (!unique.add(id)) {
    			Integer innerI = i;
    			DebtPositionErrorRow errorRow = debtPosValidationErr.getErrorRows().stream().filter(e -> e.getRowNumber() == (innerI+1)).findFirst().orElse(null);
    			if(null != errorRow) {
    				// already exist an error for the row number, add the new one
    				errorRow.getErrorsDetail().add("Duplicated ID ["+id+"] found");
    			}
    			else {
    				errorRow = new DebtPositionErrorRow();
    				errorRow.setRowNumber((innerI+1));
    				errorRow.getErrorsDetail().add("Duplicated ID ["+id+"] found");
    				debtPosValidationErr.getErrorRows().add(errorRow);
    			}
    		}
    	}
    }
    
    
}
