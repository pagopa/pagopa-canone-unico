package it.gov.pagopa.canoneunico.csv.validaton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.opencsv.bean.BeanVerifier;
import com.opencsv.exceptions.CsvConstraintViolationException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;

public class PaymentNoticeVerifier implements BeanVerifier<PaymentNotice>{
	private final Set<String> unique = new HashSet<>(); 

	@Override
	public boolean verifyBean(PaymentNotice bean) throws CsvConstraintViolationException {
		
		List<String> errors = new ArrayList<>();
		
		// check unique id
		if (!unique.add(bean.getId())) {
			errors.add("Duplicated ID '"+bean.getId()+"' found");
		}
		
		// check mutual exclusion pa_id_istat | pa_id_catasto | pa_id_fiscal_code
		if (!((null != bean.getPaIdCatasto() ^ null != bean.getPaIdIstat() ^ null!= bean.getPaIdFiscalCode())
				^ (null != bean.getPaIdCatasto() && null != bean.getPaIdIstat() && null != bean.getPaIdFiscalCode()))) {
			errors.add("Only one of pa_id_istat, pa_id_catasto and pa_id_fiscal_code can be valued.");
			
		}
		
		// TODO: check existence of the organization fiscal code
		
		if (!errors.isEmpty()) {
			throw new CsvConstraintViolationException(String.join(" # ", errors));
		}
		
		return true;
	}

}
