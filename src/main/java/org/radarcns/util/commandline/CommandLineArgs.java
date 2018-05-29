package org.radarcns.util.commandline;

import com.beust.jcommander.Parameter;

public class CommandLineArgs {

    @Parameter(names = { "-f", "--format" }, description = "Format to use when converting the files. JSON and CSV is available.")
    public String format = "csv";

    @Parameter(names = { "-c", "--compression" }, description = "Compression to use when converting the files. Gzip is available.")
    public String compression = "none";

    // Default set to false because causes loss of records from Biovotion data. https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16
    @Parameter(names = { "-d", "--deduplicate" }, description = "Boolean to define if to use deduplication or not.", validateWith = BooleanValidator.class)
    public Boolean deduplicate = false;

    @Parameter(names = { "-u", "--hdfs-uri" }, description = "The HDFS uri to connect to. Eg - 'hdfs://<HOST>:<RPC_PORT>/<PATH>'.", required = true, validateWith = { HdfsUriValidator.class, PathValidator.class })
    public String hdfsUri;

    @Parameter(names = { "-i", "--hdfs-root-directory" }, description = "The input HDFS root directory from which files are to be read. Eg - '/topicAndroidNew'", required = true, validateWith = PathValidator.class)
    public String hdfsRootDirectory;

    @Parameter(names = { "-o", "--output-directory"}, description = "The output folder where the files are to be extracted.", required = true, validateWith = PathValidator.class)
    public String outputDirectory;

    @Parameter(names = { "-h", "--help"}, help = true, description = "Display the usage of the program with available options.")
    public boolean help;
}