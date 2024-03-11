package it.gov.pagopa.canoneunico.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class BlobInfo {
    private String container;   // corporate container
    private String directory;   // one of {input, output, error} directory
    private String name;        // blob file name
    private String url;         // blob url: container/directory/blob-name
}
