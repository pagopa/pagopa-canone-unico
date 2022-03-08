package it.gov.pagopa.canoneunico.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class CsvOutModel {
    private String csvFileName;
    private List<List<String>> data;
}
