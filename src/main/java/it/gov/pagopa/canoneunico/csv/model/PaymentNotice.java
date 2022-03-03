package it.gov.pagopa.canoneunico.csv.model;

import com.opencsv.bean.CsvBindByName;

import lombok.Data;

@Data
public class PaymentNotice {

	@CsvBindByName(required = true)
    private String id;
	@CsvBindByName(column = "pa_id_istat")
	private String paIdIstat;
	@CsvBindByName(column = "pa_id_catasto")
	private String paIdCatasto;
	@CsvBindByName(column = "pa_id_fiscal_code")
	private String paIdFiscalCode;
	@CsvBindByName(column = "pa_id_cbill")
	private String paIdCBill;
	@CsvBindByName(column = "pa_pec_mail")
	private String paPecEmail;
	@CsvBindByName(column = "pa_referent_email")
	private String paReferentEmail;
	@CsvBindByName(column = "pa_referent_name")
	private String paReferentName;
	@CsvBindByName(required = true)
	private long amount;
	@CsvBindByName(column = "debtor_id_fiscal_code", required = true)
	private String debtorFiscalCode;
	@CsvBindByName(column = "debtor_name", required = true)
	private String debtorName;
	@CsvBindByName(column = "debtor_email")
	private String debtorEmail;
	@CsvBindByName(column = "payment_notice_number")
	private long paymentNoticeNumber;
	@CsvBindByName(column = "note")
	private String note;
}
