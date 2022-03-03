package it.gov.pagopa.canoneunico.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class Transfer {
    private String idTransfer;
    private Long amount;
    private String remittanceInformation;
    private String category;
    private String iban;
}
