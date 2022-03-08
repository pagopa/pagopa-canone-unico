package it.gov.pagopa.canoneunico.csv.validaton;

import com.opencsv.bean.CsvToBean;
import com.opencsv.exceptions.CsvException;
import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.model.DebtPositionValidationCsv;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class CsvValidation {

    public static DebtPositionValidationCsv checkCsvIsValid(String fileName, CsvToBean<PaymentNotice> csvToBean) {
        List<PaymentNotice> payments = csvToBean != null ? csvToBean.parse() : new ArrayList<>();
        List<CsvException> parsingExceptions = csvToBean != null ? csvToBean.getCapturedExceptions() : new ArrayList<>();
        DebtPositionValidationCsv debtPosValidation = new DebtPositionValidationCsv();
        debtPosValidation.setCsvFilename(fileName);
        debtPosValidation.setPayments(payments);
        debtPosValidation.setParsingExceptions(parsingExceptions);
        debtPosValidation.setTotalNumberRows(payments.size() + parsingExceptions.size());
        debtPosValidation.setNumberInvalidRows(parsingExceptions.size());
        CsvValidation.checkConstraintErrors(debtPosValidation);
        return debtPosValidation;
    }

    private static void checkConstraintErrors(DebtPositionValidationCsv debtPosValidationErr) {
        if (!debtPosValidationErr.getParsingExceptions().isEmpty()) {
            for (CsvException ex : debtPosValidationErr.getParsingExceptions()) {

                DebtPositionErrorRow errorRow = debtPosValidationErr.getErrorRows().stream().filter(e -> e.getRowNumber() == ex.getLineNumber()).findFirst().orElse(null);
                if (null != errorRow) {
                    // already exist an error for the row number, add the new one
                    errorRow.getErrorsDetail().add(ex.getMessage());
                } else {
                    errorRow = new DebtPositionErrorRow();
                    errorRow.setRowNumber(ex.getLineNumber());
                    errorRow.getErrorsDetail().add(ex.getMessage());
                    debtPosValidationErr.getErrorRows().add(errorRow);
                }
            }
        }

    }

}
