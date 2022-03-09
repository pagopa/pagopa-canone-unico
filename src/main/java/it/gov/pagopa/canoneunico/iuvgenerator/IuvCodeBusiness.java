package it.gov.pagopa.canoneunico.iuvgenerator;

import lombok.experimental.UtilityClass;

/**
 * Business logic class
 */
@UtilityClass
public class IuvCodeBusiness {

    /**
     * Generates a sequential <code>iuv</code>
     * 
     * @param segregationCode
     * @param nextValSequence
     * @return the <code>iuv</code>
     * @see pagopa.gov.it.toolkit.iuvGenerator.bean.IuvCodeGenerator
     * 
     */
    public static String generateIUV(Integer segregationCode, Integer nextValSequence) {
        IuvAlghoritmGenerator iuvGenerator = new IuvAlghoritmGenerator.Builder().build();
        return iuvGenerator.generate(segregationCode, nextValSequence);
    }
    
    /**
     * Generates a random <code>iuv</code>
     * 
     * @param segregationCode
     * @return the <code>iuv</code>
     * @see pagopa.gov.it.toolkit.iuvGenerator.bean.IuvCodeGenerator
     * 
     */
    public static String generateIUV(Integer segregationCode) {
        IuvAlghoritmGenerator iuvGenerator = new IuvAlghoritmGenerator.Builder().build();
        return iuvGenerator.generate(segregationCode);
    }
}
