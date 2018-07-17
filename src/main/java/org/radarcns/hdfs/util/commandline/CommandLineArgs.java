package org.radarcns.hdfs.util.commandline;

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

    @Parameter(names = { "-n", "--nameservice"}, description = "The HDFS name services to connect to. Eg - '<HOST>' for single configurations or <CLUSTER_ID> for high availability web services.", required = true, validateWith = { PathValidator.class })
    public String hdfsUri;

    @Parameter(names = { "--namenode-1" }, description = "High availability HDFS name node to also connect to. Eg - '<HOST>:<RPC_PORT>'.", validateWith = { PathValidator.class })
    public String hdfsUri1;

    @Parameter(names = { "--namenode-2" }, description = "High availability HDFS name node to also connect to. Eg - '<HOST>:<RPC_PORT>'.", validateWith = { PathValidator.class })
    public String hdfsUri2;

    @Parameter(names = { "--namenode-ha"}, description = "High availability HDFS name node names. Eg - 'nn1,nn2'.", validateWith = { PathValidator.class })
    public String hdfsHa;

    @Parameter(names = { "-o", "--output-directory"}, description = "The output folder where the files are to be extracted.", required = true, validateWith = PathValidator.class)
    public String outputDirectory;

    @Parameter(names = { "-h", "--help"}, help = true, description = "Display the usage of the program with available options.")
    public boolean help;

    @Parameter(names = { "--no-stage"}, description = "Do not stage output files into a temporary directory before moving them to the data directory. This increases performance but may leave corrupted data files.")
    public boolean noStage = false;
}