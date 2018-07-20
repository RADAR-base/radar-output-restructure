package org.radarcns.hdfs.util.commandline;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.radarcns.hdfs.Application.CACHE_SIZE_DEFAULT;

public class CommandLineArgs {

    @Parameter(description = "<input_path_1> [<input_path_2> ...]", variableArity = true, required = true)
    public List<String> inputPaths = new ArrayList<>();

    @Parameter(names = { "-f", "--format" }, description = "Format to use when converting the files. JSON and CSV are available by default.")
    public String format = "csv";

    @Parameter(names = { "-c", "--compression" }, description = "Compression to use when converting the files. Gzip is available by default.")
    public String compression = "none";

    // Default set to false because causes loss of records from Biovotion data. https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16
    @Parameter(names = { "-d", "--deduplicate" }, description = "Boolean to define if to use deduplication or not.")
    public boolean deduplicate = false;

    @Parameter(names = { "-n", "--nameservice"}, description = "The HDFS name services to connect to. Eg - '<HOST>' for single configurations or <CLUSTER_ID> for high availability web services.", required = true, validateWith = { PathValidator.class })
    public String hdfsUri;

    @Parameter(names = { "--namenode-1" }, description = "High availability HDFS first name node hostname.", validateWith = { PathValidator.class })
    public String hdfsUri1;

    @Parameter(names = { "--namenode-2" }, description = "High availability HDFS second name node hostname. Eg - '<HOST>'.", validateWith = { PathValidator.class })
    public String hdfsUri2;

    @Parameter(names = { "--namenode-ha"}, description = "High availability HDFS name node names. Eg - 'nn1,nn2'.", validateWith = { PathValidator.class })
    public String hdfsHa;

    @Parameter(names = { "-o", "--output-directory"}, description = "The output folder where the files are to be extracted.", required = true, validateWith = PathValidator.class)
    public String outputDirectory;

    @Parameter(names = { "-h", "--help"}, help = true, description = "Display the usage of the program with available options.")
    public boolean help;

    @Parameter(names = { "--path-factory" }, description = "Factory to create path names with")
    public String pathFactory;

    @Parameter(names = {"--storage-driver"}, description = "Storage driver to use for storing data")
    public String storageDriver;

    @Parameter(names = {"--format-factory"}, description = "Format factory class to use for storing data")
    public String formatFactory;

    @Parameter(names = {"--compression-factory"}, description = "Compression factory class to use for compressing/decompressing data")
    public String compressionFactory;

    @DynamicParameter(names = {"-p", "--property"}, description = "Properties used by custom plugins.")
    public Map<String, String> properties = new HashMap<>();

    @Parameter(names = {"-t", "--num-threads"}, description = "Number of threads to use for processing")
    public int numThreads = 1;

    @Parameter(names = {"-s", "--cache-size"}, description = "Number of files to keep in cache in a single thread.")
    public int cacheSize = CACHE_SIZE_DEFAULT;

    public static <T> T nonNullOrDefault(T value, Supplier<T> defaultValue) {
        return value != null ? value : defaultValue.get();
    }

    public static <T> T instantiate(String clsName, Class<T> superClass)
            throws ReflectiveOperationException, ClassCastException {
        if (clsName == null) {
            return null;
        }
        Class cls = Class.forName(clsName);
        return superClass.cast(cls.newInstance());
    }
}