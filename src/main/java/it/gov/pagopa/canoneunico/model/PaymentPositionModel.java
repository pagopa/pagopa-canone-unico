package it.gov.pagopa.canoneunico.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(toBuilder = true)
public class PaymentPositionModel {
    private String iupd;
    private String type;
    private String fiscalCode;
    private String fullName;
    private String email;
    private String companyName;
    private String validityDate;
    private Boolean switchToExpired;
    private List<PaymentOptionModel> paymentOption;
}
