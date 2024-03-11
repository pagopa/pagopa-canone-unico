package it.gov.pagopa.canoneunico.csv.validaton;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvException;

import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CsvValidation {

    public static DebtPositionValidationCsv checkCsvIsValid(Logger logger, String fileName, CsvToBean<PaymentNotice> csvToBean){
		DebtPositionValidationCsv debtPosValidation = new DebtPositionValidationCsv();
		debtPosValidation.setCsvFilename(fileName);
		List<PaymentNotice> payments = new ArrayList<>();

		if(csvToBean != null) {
			try {
				payments = csvToBean.parse();
			} catch (Exception e) {
				logger.log(Level.INFO, () -> e.getCause() + " " + e.getMessage());
				// to work around the error "the exception is never thrown in the corresponding try block", actually the exception is thrown in specific cases
				if(String.valueOf(e.getCause()).contains("CsvRequiredFieldEmptyException"))
					csvToBean.getCapturedExceptions().add(
							new CsvRequiredFieldEmptyException(
									String.format("%s %s", e.getMessage(), e.getCause())
							)
					);
			}
		}

		List<CsvException> parsingExceptions = csvToBean!=null ? csvToBean.getCapturedExceptions(): new ArrayList<>();
		debtPosValidation.setPayments(payments);
		debtPosValidation.setParsingExceptions(parsingExceptions);
		debtPosValidation.setTotalNumberRows(payments.size()+parsingExceptions.size());
		debtPosValidation.setNumberInvalidRows(parsingExceptions.size());
    	CsvValidation.checkConstraintErrors(debtPosValidation);
        return debtPosValidation;
    }
    
    private static void checkConstraintErrors (DebtPositionValidationCsv debtPosValidationErr) {
    	if (!debtPosValidationErr.getParsingExceptions().isEmpty()) {
    		for (CsvException ex: debtPosValidationErr.getParsingExceptions()) {

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
