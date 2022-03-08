package it.gov.pagopa.canoneunico.iuvgenerator.exception;

import javax.validation.ConstraintViolation;
import java.util.Set;

/**
 * Validation exception thrown in case of validation error
 */
public class ValidationException extends IllegalArgumentException {


    public static final String VALIDATION_ERROR = "Validation Errors: ";
    /**
     * generated serialVersionUID
     */
    private static final long serialVersionUID = -7866813409413570366L;

    public <T> ValidationException(Set<ConstraintViolation<T>> validationInputResults) {
        super(generateErrorMessage(validationInputResults));
    }

    /**
     *
     */
    public ValidationException() {
    }

    /**
     * @param s
     */
    public ValidationException(String s) {
        super(s);
    }

    /**
     * @param cause
     */
    public ValidationException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Validation by annotation
     *
     * @param validationInputResults List of validation errors
     * @return a string containing the list of validation errors
     * @see pagopa.gov.it.toolkit.iuvGenerator.validation.IuvCodeValidationImpl
     */
    private static <T> String generateErrorMessage(Set<ConstraintViolation<T>> validationInputResults) {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append(VALIDATION_ERROR);


        for (ConstraintViolation<T> validationError : validationInputResults) {
            String validationErrorMsg = validationError.getPropertyPath() + "[" + validationError.getMessage() + "]; ";
            errorMsg.append(errorMsg);
            errorMsg.append(validationErrorMsg);
        }

        return errorMsg.toString();
    }
}
