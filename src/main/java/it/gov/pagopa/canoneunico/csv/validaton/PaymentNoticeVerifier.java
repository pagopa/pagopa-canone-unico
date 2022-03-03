package it.gov.pagopa.canoneunico.csv.validaton;

import com.opencsv.bean.BeanVerifier;
import com.opencsv.exceptions.CsvConstraintViolationException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;

public class PaymentNoticeVerifier implements BeanVerifier<PaymentNotice>{

	@Override
	public boolean verifyBean(PaymentNotice bean) throws CsvConstraintViolationException {
		
		// check mutual exclusion pa_id_istat | pa_id_catasto | pa_id_fiscal_code
		if (!((null != bean.getPaIdCatasto() ^ null != bean.getPaIdIstat() ^ null!= bean.getPaIdFiscalCode())
				^ (null != bean.getPaIdCatasto() && null != bean.getPaIdIstat() && null != bean.getPaIdFiscalCode()))) {
			throw new CsvConstraintViolationException("Only one of pa_id_istat, pa_id_catasto and pa_id_fiscal_code can be valued.");
		}
		
		// check existence of the organization fiscal code
		return true;
	}

}
