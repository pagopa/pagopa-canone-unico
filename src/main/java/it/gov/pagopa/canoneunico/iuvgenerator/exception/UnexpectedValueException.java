package it.gov.pagopa.canoneunico.iuvgenerator.exception;

/**
 * Unexpected value exception thrown in case of an error during the extraction
 * of IUV base value
 */
public class UnexpectedValueException extends IllegalArgumentException {


    /**
	 * generated serialVersionUID
	 */
	private static final long serialVersionUID = 4866573387331033063L;

	/**
     * 
     */
    public UnexpectedValueException() {
    }

    /**
     * @param s
     */
    public UnexpectedValueException(String s) {
        super(s);
    }

    /**
     * @param cause
     */
    public UnexpectedValueException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public UnexpectedValueException(String message, Throwable cause) {
        super(message, cause);
    }
}
