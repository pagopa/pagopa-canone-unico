package it.gov.pagopa.canoneunico.entity;

import com.microsoft.azure.storage.table.TableServiceEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DebtPositionEntity extends TableServiceEntity {

    private String status;

    // CSV
    private String paIdIstat;
    private String paIdCatasto;
    private String paIdFiscalCode;
    private String paIdCbill;
    private String paPecEmail;
    private String paReferentEmail;
    private String paReferentName;
    private String debtorIdFiscalCode;
    private String paymentNoticeNumber;
    private String note;

    private String debtorName;
    private String debtorEmail;
    private String amount;

    // generated
    private String iuv;
    private String iupd;

    // EC config
    private String fiscalCode;
    private String companyName;
    private String iban;


    public DebtPositionEntity(String filename, String id) {
        this.partitionKey = filename;
        this.rowKey = id;
    }

}
