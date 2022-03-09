package it.gov.pagopa.canoneunico.entity;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class IuvEntity extends TableServiceEntity {
    
    public IuvEntity(String paIdFiscalCode, String iuv) {
        this.partitionKey = paIdFiscalCode;
        this.rowKey = iuv;
        // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.table.tableentity.etag?view=azure-dotnet#microsoft-azure-cosmos-table-tableentity-etag
        this.etag = "*";
    }

}
