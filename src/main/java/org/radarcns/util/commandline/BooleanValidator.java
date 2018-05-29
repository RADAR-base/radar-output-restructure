package org.radarcns.util.commandline;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class BooleanValidator implements IParameterValidator{

    @Override
    public void validate(String name, String value) throws ParameterException {
        if(!value.equalsIgnoreCase("true")
                && !value.equalsIgnoreCase("false")) {
            throw new ParameterException("Parameter " + name + " can only be true or "
                    + "false (found " + value +"). "
                    + "Please run with --help or -h for more information.");
        }
    }
}
