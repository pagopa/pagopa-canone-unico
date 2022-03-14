package it.gov.pagopa.canoneunico.entity;

import com.microsoft.azure.storage.table.TableServiceEntity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class EcConfigEntity extends TableServiceEntity {
    
	private String paIdIstat;
    private String paIdCatasto;
    private String paIdCbill;
    private String paPecEmail;
    private String paReferentEmail;
    private String paReferentName;
    private String companyName;
    private String iban;
    
    
	
    public EcConfigEntity(String paIdFiscalCode) {
        this.partitionKey = "org";
        this.rowKey = paIdFiscalCode;
        // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.table.tableentity.etag?view=azure-dotnet#microsoft-azure-cosmos-table-tableentity-etag
        this.etag = "*";
    }

}
