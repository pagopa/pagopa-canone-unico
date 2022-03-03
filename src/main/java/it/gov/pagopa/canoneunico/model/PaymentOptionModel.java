package it.gov.pagopa.canoneunico.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(toBuilder = true)
public class PaymentOptionModel {
    private String iuv;
    private Long amount;
    private String type;
    private String description;
    private Boolean isPartialPayment;
    private String dueDate;
    private List<Transfer> transfer;
}
