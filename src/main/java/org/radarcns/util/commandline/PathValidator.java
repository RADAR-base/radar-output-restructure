package org.radarcns.util.commandline;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.IParameterValidator;

public class PathValidator implements IParameterValidator{
    @Override
    public void validate(String name, String value) throws ParameterException {
        if (value == null || value.isEmpty()) {
            throw new ParameterException("Parameter " + name + " should be supplied. "
                    + "It cannot be empty or null. (found " + value +")."
                    + "Please run with --help or -h for more information.");
        }
    }
}
