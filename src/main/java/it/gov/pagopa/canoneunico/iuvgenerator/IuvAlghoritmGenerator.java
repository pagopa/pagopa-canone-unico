package it.gov.pagopa.canoneunico.iuvgenerator;

import it.gov.pagopa.canoneunico.iuvgenerator.exception.ValidationException;

/**
 * IUV code generator with IUV alghoritm interface
 */
public interface IuvAlghoritmGenerator {

    /**
     * Generates the IUV Code
     *
     * @param segregationCode the segregation code
     * @return the IUV Code
     */
    String generate(Integer segregationCode, String nextValSequence);

    /**
     * Initialization of <code>IuvAlghoritmGenerator</code> class
     */
    class Builder {

        /**
         * Build the IuvAlghoritmGenerator based on <code>auxDigit</code>
         *
         * @return a new instance of <code>IuvAlghoritmGenerator</code>
         */
        public IuvAlghoritmGenerator build() throws ValidationException {
            return new IuvAlghoritmAuxDigit3();
        }
    }
}
