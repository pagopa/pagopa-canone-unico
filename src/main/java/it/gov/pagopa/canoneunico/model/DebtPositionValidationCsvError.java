package it.gov.pagopa.canoneunico.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import it.gov.pagopa.canoneunico.model.error.DebtPositionErrorRow;


@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DebtPositionValidationCsvError {

    private String csvFilename;
    private Integer totalNumberRows;
    private Integer numberInvalidRows;
    @Builder.Default
    private List<DebtPositionErrorRow> errorRows = new ArrayList<>();

}
