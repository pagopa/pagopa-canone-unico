package it.gov.pagopa.canoneunico.csv.validaton;

import it.gov.pagopa.canoneunico.iuvgenerator.IuvCodeGenerator;
import it.gov.pagopa.canoneunico.iuvgenerator.exception.ValidationException;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Optional;
import java.util.Set;


/**
 * Implementation of IuvCodeValidation interface
 */
public class IuvCodeValidationImpl implements IuvCodeValidation {

    public static final String VALIDATION_SEGREGATION_CODE_ERROR = "SegregationCode cannot be null for AuxDigit 3";

    /**
     * Validate the debtPosition<br/>
     * The validation includes:
     * <ul>
     * <li>checkConstraints - validation by annotation
     * <li>checkAuxDigit3 - if <code>auxDigit</code> = 3
     * <code>segregationCode</code> must be present
     * </ul>
     *
     * @param iuvCodeGenerator the bean to validate
     * @throws ValidationException
     * @see IuvCodeGenerator
     * @see ValidationException
     */
    @Override
    public void validate(IuvCodeGenerator iuvCodeGenerator) throws ValidationException {
        checkConstraints(iuvCodeGenerator);

        checkAuxDigit3(iuvCodeGenerator);
    }

    /**
     * @param objectToValidate the bean to validate.
     * @throws ValidationException
     * @see ValidationException
     */
    public <T> void checkConstraints(T objectToValidate) throws ValidationException {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        Set<ConstraintViolation<T>> validationInputResults = validator.validate(objectToValidate);
        if (!validationInputResults.isEmpty()) {
            throw new ValidationException(validationInputResults);
        }
    }

    /**
     * @param iuvCodeGenerator
     * @see IuvCodeGenerator
     */
    private void checkAuxDigit3(IuvCodeGenerator iuvCodeGenerator) {
        if (iuvCodeGenerator.getAuxDigit() == 3 &&
                Optional.ofNullable(iuvCodeGenerator.getSegregationCode())
                        .orElse(0) == 0) {
            throw new ValidationException(VALIDATION_SEGREGATION_CODE_ERROR);
        }

    }
}
