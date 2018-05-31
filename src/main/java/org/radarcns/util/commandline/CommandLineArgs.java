package org.radarcns.util.commandline;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class CommandLineArgs {

    @Parameter(description = "<input_path_1> [<input_path_2> ...]", variableArity = true, required = true)
    public List<String> inputPaths = new ArrayList<>();

    @Parameter(names = { "-f", "--format" }, description = "Format to use when converting the files. JSON and CSV is available.")
    public String format = "csv";

    @Parameter(names = { "-c", "--compression" }, description = "Compression to use when converting the files. Gzip is available.")
    public String compression = "none";

    // Default set to false because causes loss of records from Biovotion data. https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16
    @Parameter(names = { "-d", "--deduplicate" }, description = "Boolean to define if to use deduplication or not.")
    public boolean deduplicate;

    @Parameter(names = { "-u", "--hdfs-uri" }, description = "The HDFS uri to connect to. Eg - 'hdfs://<HOST>:<RPC_PORT>/<PATH>'.", required = true, validateWith = { HdfsUriValidator.class, PathValidator.class })
    public String hdfsUri;

    @Parameter(names = { "-o", "--output-directory"}, description = "The output folder where the files are to be extracted.", required = true, validateWith = PathValidator.class)
    public String outputDirectory;

    @Parameter(names = { "-h", "--help"}, help = true, description = "Display the usage of the program with available options.")
    public boolean help;
}