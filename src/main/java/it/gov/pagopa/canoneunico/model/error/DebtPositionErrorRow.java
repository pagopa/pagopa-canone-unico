package it.gov.pagopa.canoneunico.model.error;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DebtPositionErrorRow {
    private long rowNumber;
    private List<String> errorsDetail = new ArrayList<>();
}
