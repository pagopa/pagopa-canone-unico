package it.gov.pagopa.canoneunico.csv.model;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

@Data
public class PaymentNoticeError {

    @CsvBindByName
    @CsvBindByPosition(position = 0)
    private String id;
    @CsvBindByName(column = "pa_id_istat")
    @CsvBindByPosition(position = 1)
    private String paIdIstat;
    @CsvBindByName(column = "pa_id_catasto")
    @CsvBindByPosition(position = 2)
    private String paIdCatasto;
    @CsvBindByName(column = "pa_id_fiscal_code")
    @CsvBindByPosition(position = 3)
    private String paIdFiscalCode;
    @CsvBindByName(column = "pa_id_cbill")
    @CsvBindByPosition(position = 4)
    private String paIdCBill;
    @CsvBindByName(column = "pa_pec_mail")
    @CsvBindByPosition(position = 5)
    private String paPecEmail;
    @CsvBindByName(column = "pa_referent_email")
    @CsvBindByPosition(position = 6)
    private String paReferentEmail;
    @CsvBindByName(column = "pa_referent_name")
    @CsvBindByPosition(position = 7)
    private String paReferentName;
    @CsvBindByName
    @CsvBindByPosition(position = 8)
    private long amount;
    @CsvBindByName(column = "debtor_id_fiscal_code")
    @CsvBindByPosition(position = 9)
    private String debtorFiscalCode;
    @CsvBindByName(column = "debtor_name")
    @CsvBindByPosition(position = 10)
    private String debtorName;
    @CsvBindByName(column = "debtor_email")
    @CsvBindByPosition(position = 11)
    private String debtorEmail;
    @CsvBindByName(column = "payment_notice_number")
    @CsvBindByPosition(position = 12)
    private long paymentNoticeNumber;
    @CsvBindByName(column = "note")
    @CsvBindByPosition(position = 13)
    private String note;
    @CsvBindByName(column = "errors_note")
    @CsvBindByPosition(position = 14)
    private String errorsNote = "N/A";
}
