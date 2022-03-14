package it.gov.pagopa.canoneunico.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import com.opencsv.exceptions.CsvException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;


@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DebtPositionValidationCsv {

    private String csvFilename;
    private Integer totalNumberRows;
    private Integer numberInvalidRows;
    private List<PaymentNotice> payments;
	private List<CsvException> parsingExceptions;
    @Builder.Default
    private List<DebtPositionErrorRow> errorRows = new ArrayList<>();

}
