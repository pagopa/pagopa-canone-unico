package it.gov.pagopa.canoneunico.model.error;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class DebtPositionErrorRow {
	private long rowNumber;
    private List<String> errorsDetail = new ArrayList<>();
}
