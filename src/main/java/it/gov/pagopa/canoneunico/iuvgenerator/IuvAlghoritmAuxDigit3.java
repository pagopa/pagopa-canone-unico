package it.gov.pagopa.canoneunico.iuvgenerator;

import java.text.DecimalFormat;

/**
 * IUV code generation algorithm based on <code>auxDigit</code> = 3
 */
public class IuvAlghoritmAuxDigit3 extends IuvAlghoritm {

    private int auxDigit = 3;

    /**
     * Protected constructor
     */
    protected IuvAlghoritmAuxDigit3() {
        // NOPE
    }

    /**
     * Generate the IUV Code.<br/>
     * IUV (17 digits) = &lt;codice segregazione (2n)&gt;&lt;IUV base (max
     * 13n)&gt;&lt;IUV check digit (2n)&gt;
     */
    @Override
    public String generate(Integer segregationCode, Integer nextValSequence) {
        String segregationCodeString = new DecimalFormat("00").format(segregationCode);
        String iuvBase13Digits = generateSeqIuv13Digits(nextValSequence);
        String checkDigit = generateCheckDigit(String.valueOf(auxDigit) + segregationCodeString + iuvBase13Digits);
        return segregationCodeString + iuvBase13Digits + checkDigit;
    }
    
    @Override
    public String generate(Integer segregationCode) {
        String segregationCodeString = new DecimalFormat("00").format(segregationCode);
        String iuvBase13Digits = generateRandomIuv13Digits();
        String checkDigit = generateCheckDigit(String.valueOf(auxDigit) + segregationCodeString + iuvBase13Digits);
        return segregationCodeString + iuvBase13Digits + checkDigit;
    }
}