package it.gov.pagopa.canoneunico.csv.validaton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;

import com.opencsv.bean.BeanVerifier;
import com.opencsv.exceptions.CsvConstraintViolationException;

import it.gov.pagopa.canoneunico.csv.model.PaymentNotice;
import it.gov.pagopa.canoneunico.entity.EcConfigEntity;

public class PaymentNoticeVerifier implements BeanVerifier<PaymentNotice>{
	private final Set<String> unique = new HashSet<>(); 
	private List<EcConfigEntity> organizationsList = new ArrayList<>();
	
	public PaymentNoticeVerifier (List<EcConfigEntity> organizationsList) {
		this.organizationsList = organizationsList;
	}

	@Override
	public boolean verifyBean(PaymentNotice bean) throws CsvConstraintViolationException {
		
		List<String> errors = new ArrayList<>();
		
		// check unique id
		if (!unique.add(bean.getId())) {
			errors.add("Duplicated ID '"+bean.getId()+"' found.");
		}
		
		// check mutual exclusion pa_id_istat | pa_id_catasto | pa_id_fiscal_code
		if (!((null != bean.getPaIdCatasto() ^ null != bean.getPaIdIstat() ^ null!= bean.getPaIdFiscalCode())
				^ (null != bean.getPaIdCatasto() && null != bean.getPaIdIstat() && null != bean.getPaIdFiscalCode()))) {
			errors.add("Only one of pa_id_istat, pa_id_catasto and pa_id_fiscal_code can be valued.");
			
		}
		
		// check existence of the organization fiscal code
		if (null != bean.getPaIdFiscalCode() && !bean.getPaIdFiscalCode().isBlank() && !checkIsPresentOrganizationFiscalCode(bean.getPaIdFiscalCode())) {
			errors.add("Not found the pa_id_fiscal_code ["+bean.getPaIdFiscalCode()+"].");
		}
		
		if (!errors.isEmpty()) {
			throw new CsvConstraintViolationException(String.join(" # ", errors));
		}
		
		return true;
	}
	
	private boolean checkIsPresentOrganizationFiscalCode(String paIdFiscalCode) {
		Optional<EcConfigEntity> ecConfig = organizationsList.stream().filter(o -> o.getRowKey().equals(paIdFiscalCode)).findFirst();
		return ecConfig.isPresent();
	}

}
