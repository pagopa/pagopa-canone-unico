package it.gov.pagopa.canoneunico.iuvgenerator;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

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
     * Generates the IUV base 13 digits
     * 
     * @return the IUV base
     */
    protected String generateIuBase13Digits(String nextValSequence) {
	String sequence=nextValSequence;
	
	sequence = StringUtils.leftPad(sequence, 9, '0');
        sequence = Calendar.getInstance().get(Calendar.YEAR) + sequence;

        if (!pattern.matcher(sequence).matches()) {
            throw new UnexpectedValueException(UNEXPECTED_GENERATED_VALUE_ERROR + sequence);
        }
        
        return sequence;
    }
}