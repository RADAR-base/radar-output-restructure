package org.radarcns.util.commandline;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.IParameterValidator;

public class PathValidator implements IParameterValidator{
    @Override
    public void validate(String name, String value) throws ParameterException {
        if (value == null || value.isEmpty()) {
            ParameterException exc =  new ParameterException("Parameter " + name + " should be supplied. It cannot be empty or null. (found " + value +")");
            exc.usage();
            throw exc;
        }
    }
}
