package it.gov.pagopa.canoneunico.entity;

import com.microsoft.azure.storage.table.TableServiceEntity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper=false)
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class IuvNumber extends TableServiceEntity {

    private Long id;
    private String idDominioPa;
    private Long lastUsedNumber;
    private Integer anno;

}
