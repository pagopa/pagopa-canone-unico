package it.gov.pagopa.canoneunico.entity;

import com.microsoft.azure.storage.table.TableServiceEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class DebtPositionEntity extends TableServiceEntity {

    // {@link Status}
    private String status; // Status: INSERTED | CREATED | ERROR

    // CSV
    private String paIdIstat;
    private String paIdCatasto;
    private String paIdFiscalCode;
    private String paIdCbill;
    private String paPecEmail;
    private String paReferentEmail;
    private String paReferentName;
    private String debtorIdFiscalCode;
    private String note;

    private String debtorName;
    private String debtorEmail;
    private String amount; // Long

    // generated
    private String paymentNoticeNumber;
    private String iupd;

    // EC config
    private String fiscalCode;
    private String companyName;
    private String iban;


    public DebtPositionEntity(String filename, String id) {
        this.partitionKey = filename;
        this.rowKey = id;
        // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.table.tableentity.etag?view=azure-dotnet#microsoft-azure-cosmos-table-tableentity-etag
        this.etag = "*";
    }

}
