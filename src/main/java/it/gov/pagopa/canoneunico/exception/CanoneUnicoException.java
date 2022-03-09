package it.gov.pagopa.canoneunico.exception;

public class CanoneUnicoException extends RuntimeException {

	/**
	 * generated serialVersionUID
	 */
	private static final long serialVersionUID = -7224755899401487270L;
	
	/**
     * Constructor with no detail message.
     */
    public CanoneUnicoException() {
        super();
    }

    /**
     * Constructor with the specified detail
     * message.
     * A detail message is a String that describes this particular
     * exception.
     *
     * @param msg the detail message.
     */
    public CanoneUnicoException(String msg) {
        super(msg);
    }

    /**
     * Constructor with the specified
     * detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method).
     * @param cause the cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A {@code null} value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     * @since 1.5
     */
    public CanoneUnicoException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor with the specified cause
     * and a detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of
     * {@code cause}).
     *
     * @param cause the cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A {@code null} value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     * @since 1.5
     */
    public CanoneUnicoException(Throwable cause) {
        super(cause);
    }

}
