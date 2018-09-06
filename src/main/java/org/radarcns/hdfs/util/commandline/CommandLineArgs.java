/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.hdfs.util.commandline;

import static org.radarcns.hdfs.Application.CACHE_SIZE_DEFAULT;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.validators.PositiveInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.radarcns.hdfs.Plugin;

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

    @Parameter(names = { "-n", "--nameservice"}, description = "The HDFS name services to connect to. Eg - '<HOST>' for single configurations or <CLUSTER_ID> for high availability web services.", required = true, validateWith = NonEmptyValidator.class)
    public String hdfsName;

    @Parameter(names = { "--namenode-1" }, description = "High availability HDFS first name node hostname.", validateWith = NonEmptyValidator.class)
    public String hdfsUri1;

    @Parameter(names = { "--namenode-2" }, description = "High availability HDFS second name node hostname. Eg - '<HOST>'.", validateWith = NonEmptyValidator.class)
    public String hdfsUri2;

    @Parameter(names = { "--namenode-ha"}, description = "High availability HDFS name node names. Eg - 'nn1,nn2'.", validateWith = NonEmptyValidator.class)
    public String hdfsHa;

    @Parameter(names = { "-o", "--output-directory"}, description = "The output folder where the files are to be extracted.", required = true, validateWith = NonEmptyValidator.class)
    public String outputDirectory;

    @Parameter(names = { "-h", "--help"}, help = true, description = "Display the usage of the program with available options.")
    public boolean help;

    @Parameter(names = { "--path-factory" }, description = "Factory to create path names with", converter = InstantiatePluginConverter.class)
    public Plugin pathFactory;

    @Parameter(names = {"--storage-driver"}, description = "Storage driver to use for storing data", converter = InstantiatePluginConverter.class)
    public Plugin storageDriver;

    @Parameter(names = {"--format-factory"}, description = "Format factory class to use for storing data", converter = InstantiatePluginConverter.class)
    public Plugin formatFactory;

    @Parameter(names = {"--compression-factory"}, description = "Compression factory class to use for compressing/decompressing data", converter = InstantiatePluginConverter.class)
    public Plugin compressionFactory;

    @DynamicParameter(names = {"-p", "--property"}, description = "Properties used by custom plugins.")
    public Map<String, String> properties = new HashMap<>();

    @Parameter(names = {"-t", "--num-threads"}, description = "Number of threads to use for processing", validateWith = PositiveInteger.class)
    public int numThreads = 1;

    @Parameter(names = {"--timer"}, description = "Enable timers")
    public boolean enableTimer = false;

    @Parameter(names = {"--tmp-dir"}, description = "Temporary staging directory")
    public String tmpDir = null;

    @Parameter(names = {"-s", "--cache-size"}, description = "Number of files to keep in cache in a single thread.", validateWith = PositiveInteger.class)
    public int cacheSize = CACHE_SIZE_DEFAULT;

    public static <T> T nonNullOrDefault(T value, Supplier<T> defaultValue) {
        return value != null ? value : defaultValue.get();
    }

    public static class InstantiatePluginConverter extends BaseConverter<Plugin> {
        public InstantiatePluginConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Plugin convert(String value) {
            try {
                Class cls = Class.forName(value);
                return (Plugin)cls.newInstance();
            } catch (ReflectiveOperationException | ClassCastException ex) {
                throw new ParameterException("Cannot convert " + value + " to Plugin instance", ex);
            }
        }
    }
}