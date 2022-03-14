package it.gov.pagopa.canoneunico.iuvgenerator;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.regex.Pattern;

import it.gov.pagopa.canoneunico.iuvgenerator.exception.UnexpectedValueException;

/**
 * IUV alghoritm abstract class
 */
public abstract class IuvAlghoritm implements IuvAlghoritmGenerator {
	
	public static final String UNEXPECTED_GENERATED_VALUE_ERROR = "Unexpected generated value: ";

    private static final String DIGIT_OF_2 = "%02d";
    private static final String DIGIT_OF_13 = "\\d{13}";

    private static Pattern pattern = Pattern.compile(DIGIT_OF_13);

    /**
     * Calculates the check digit of IUV code
     * 
     * @param checkDigitComponent
     *            check digit component
     * @return the generated check digit
     */
    protected String generateCheckDigit(String checkDigitComponent) {
        return String.format(DIGIT_OF_2,
                (new BigDecimal(checkDigitComponent).remainder(new BigDecimal(93))).intValue());
    }

    /**
     * Generates sequential 13 digits IUV
     * 
     * @return the IUV base
     */
    protected String generateSeqIuv13Digits(int nextValSequence) {

    	long timeStampMillis = Instant.now().toEpochMilli() + nextValSequence;	

    	String sequence=Long.toString(timeStampMillis);


    	if (!pattern.matcher(sequence).matches()) {
    		throw new UnexpectedValueException(UNEXPECTED_GENERATED_VALUE_ERROR + sequence);
    	}

    	return sequence;
    }
    
    /**
     * Generates random 13 digits IUV
     * 
     * @return the IUV base
     */
    protected String generateRandomIuv13Digits() {

    	long timeStampMillis = Instant.now().toEpochMilli();	
    	long moduleDigitis = timeStampMillis % 999999999;

    	SecureRandom sr = new SecureRandom();
    	Integer randomInt = sr.nextInt(10000);

    	String sequence = String.format("%09d", moduleDigitis) + String.format("%04d", randomInt);

    	if (!pattern.matcher(sequence).matches()) {
    		throw new UnexpectedValueException(UNEXPECTED_GENERATED_VALUE_ERROR + sequence);
    	}

    	return sequence;
    }
}