package it.gov.pagopa.canoneunico.iuvgenerator;

/**
 * IUV code generator with IUV alghoritm interface
 */
public interface IuvAlghoritmGenerator {

    /**
     * Initialization of <code>IuvAlghoritmGenerator</code> class
     */
    public static class Builder {

        /**
         * Build the IuvAlghoritmGenerator based on <code>auxDigit</code>
         * 
         * @return a new instance of <code>IuvAlghoritmGenerator</code>
         */
        public IuvAlghoritmGenerator build() {
            return new IuvAlghoritmAuxDigit3();
        }
    }

    /**
     * Generates the IUV Code
     * 
     * @param segregationCode
     *            the segregation code
     * @return the IUV Code
     */
    String generate(Integer segregationCode, Integer nextValSequence);
    
    /**
     * Generates the IUV Code
     * 
     * @param segregationCode
     *            the segregation code
     * @return the IUV Code
     */
    String generate(Integer segregationCode);
}