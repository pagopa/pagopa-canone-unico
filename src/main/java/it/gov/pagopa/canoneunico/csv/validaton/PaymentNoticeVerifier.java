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
	private static final int DEBTOR_FISCAL_CODE_LENGTH = 11;

	public PaymentNoticeVerifier (List<EcConfigEntity> organizationsList) {
		this.organizationsList = organizationsList;
	}

	@Override
	public boolean verifyBean(PaymentNotice bean) throws CsvConstraintViolationException {
		
		final String EC_CONFIG_TABLE = "in ec config table.";
		
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
		
		// check amount value
		if (bean.getAmount() <= 0) {
			errors.add("The amount must be greater than zero.");
		}

		// check debtor fiscal code length
		if (bean.getDebtorFiscalCode().length() != DEBTOR_FISCAL_CODE_LENGTH) {
			errors.add(String.format("The debtor fiscal code length must be equal to %s.", DEBTOR_FISCAL_CODE_LENGTH));
		}
		
		// check existence of the organization fiscal code if valued in csv file
		if (null != bean.getPaIdFiscalCode() && !bean.getPaIdFiscalCode().isBlank() && !checkIsPresentOrganizationFiscalCode(bean.getPaIdFiscalCode())) {
			errors.add("Not found the pa_id_fiscal_code ["+bean.getPaIdFiscalCode()+"] " + EC_CONFIG_TABLE);
		}
		
		// check existence of id istat if valued in csv file
		if (null != bean.getPaIdIstat() && !bean.getPaIdIstat().isBlank() && !checkIsPresentIdIstat(bean.getPaIdIstat())) {
			errors.add("Not found the pa_id_istat ["+bean.getPaIdIstat()+"] " + EC_CONFIG_TABLE);
		}
		
		// check existence of id catasto if valued in csv file
		if (null != bean.getPaIdCatasto() && !bean.getPaIdCatasto().isBlank() && !checkIsPresentIdCatasto(bean.getPaIdCatasto())) {
			errors.add("Not found the pa_id_catasto ["+bean.getPaIdCatasto()+"] " + EC_CONFIG_TABLE);
		}
		
		// check duplication id istat if valued in csv file
		if (null != bean.getPaIdIstat() && !bean.getPaIdIstat().isBlank() && !checkDuplicatedIdIstat(bean.getPaIdIstat())) {
			errors.add("Found duplicate pa_id_istat ["+bean.getPaIdIstat()+"] " + EC_CONFIG_TABLE);
		}
		
		// check duplication id catasto code if valued in csv file
		if (null != bean.getPaIdCatasto() && !bean.getPaIdCatasto().isBlank() && !checkDuplicatedIdCatasto(bean.getPaIdCatasto())) {
			errors.add("Found duplicate pa_id_catasto ["+bean.getPaIdCatasto()+"] " + EC_CONFIG_TABLE);
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
	
	private boolean checkIsPresentIdIstat(String idIstat) {
		Optional<EcConfigEntity> ecConfig = organizationsList.stream().filter(o -> o.getPaIdIstat()!=null && o.getPaIdIstat().equals(idIstat)).findFirst();
		return ecConfig.isPresent();
	}
	
	private boolean checkIsPresentIdCatasto(String idCatasto) {
		Optional<EcConfigEntity> ecConfig = organizationsList.stream().filter(o -> o.getPaIdCatasto()!=null && o.getPaIdCatasto().equals(idCatasto)).findFirst();
		return ecConfig.isPresent();
	}
	
	private boolean checkDuplicatedIdIstat(String idIstat) {
		long numOccurence = organizationsList.stream().filter(o -> o.getPaIdIstat()!=null && o.getPaIdIstat().equals(idIstat)).count();
		return numOccurence < 2;
	}
	
	private boolean checkDuplicatedIdCatasto(String idCatasto) {
		long numOccurence = organizationsList.stream().filter(o -> o.getPaIdCatasto()!=null && o.getPaIdCatasto().equals(idCatasto)).count();
		return numOccurence < 2;
	}

}
