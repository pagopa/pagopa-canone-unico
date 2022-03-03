package it.gov.pagopa.canoneunico.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DebtPositionRowMessage {

    // CSV
    private String id;
    private String debtorName;
    private String debtorEmail;
    private Long amount;

    // generated
    private String iuv;
    private String iupd;

    // EC config
    private String fiscalCode;
    private String companyName;
    private String iban;

}
