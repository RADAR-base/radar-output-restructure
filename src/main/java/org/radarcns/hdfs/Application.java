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

package org.radarcns.hdfs;

import static org.radarcns.hdfs.util.ProgressBar.formatTime;
import static org.radarcns.hdfs.util.commandline.CommandLineArgs.nonNullOrDefault;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.config.HdfsSettings;
import org.radarcns.hdfs.config.RestructureSettings;
import org.radarcns.hdfs.data.Compression;
import org.radarcns.hdfs.data.CompressionFactory;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.FormatFactory;
import org.radarcns.hdfs.data.LocalStorageDriver;
import org.radarcns.hdfs.data.RecordConverterFactory;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.Timer;
import org.radarcns.hdfs.util.commandline.CommandLineArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main application. */
public class Application implements FileStoreFactory {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    public static final int CACHE_SIZE_DEFAULT = 100;

    private final StorageDriver storageDriver;
    private final RecordConverterFactory converterFactory;
    private final Compression compression;
    private final HdfsSettings hdfsSettings;
    private final RecordPathFactory pathFactory;
    private final List<String> inputPaths;
    private final RestructureSettings settings;

    private Application(Builder builder) {
        this.storageDriver = builder.storageDriver;
        this.settings = builder.settings;

        converterFactory = builder.formatFactory.get(settings.getFormat());
        compression = builder.compressionFactory.get(settings.getCompression());

        pathFactory = builder.pathFactory;
        String extension = converterFactory.getExtension() + compression.getExtension();
        this.pathFactory.setExtension(extension);
        this.pathFactory.setRoot(settings.getOutputPath());

        this.inputPaths = builder.inputPaths;

        hdfsSettings = builder.hdfsSettings;
    }

    public static void main(String [] args) {
        final CommandLineArgs commandLineArgs = new CommandLineArgs();
        final JCommander parser = JCommander.newBuilder().addObject(commandLineArgs).build();

        parser.setProgramName("radar-hdfs-restructure");
        try {
            parser.parse(args);
        } catch (ParameterException ex) {
            logger.error(ex.getMessage());
            parser.usage();
            System.exit(1);
        }

        if (commandLineArgs.help) {
            parser.usage();
            System.exit(0);
        }

        logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        logger.info("Starting...");

        Timer.getInstance().setEnabled(commandLineArgs.enableTimer);

        if (commandLineArgs.enableTimer) {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> System.out.println(Timer.getInstance()), "Timer"));
        }

        Application application;

        try {
            RestructureSettings settings = new RestructureSettings.Builder(commandLineArgs.outputDirectory)
                    .compression(commandLineArgs.compression)
                    .cacheSize(commandLineArgs.cacheSize)
                    .format(commandLineArgs.format)
                    .doDeduplicate(commandLineArgs.deduplicate)
                    .tempDir(commandLineArgs.tmpDir)
                    .numThreads(commandLineArgs.numThreads)
                    .maxFilesPerTopic(commandLineArgs.maxFilesPerTopic)
                    .excludeTopics(commandLineArgs.excludeTopics)
                    .build();

            HdfsSettings hdfsSettings = new HdfsSettings.Builder(commandLineArgs.hdfsName)
                    .hdfsHighAvailability(commandLineArgs.hdfsHa,
                            commandLineArgs.hdfsUri1, commandLineArgs.hdfsUri2)
                    .build();

            application = new Builder(settings)
                    .hdfsSettings(hdfsSettings)
                    .pathFactory(commandLineArgs.pathFactory)
                    .compressionFactory(commandLineArgs.compressionFactory)
                    .formatFactory(commandLineArgs.formatFactory)
                    .storageDriver(commandLineArgs.storageDriver)
                    .properties(commandLineArgs.properties)
                    .inputPaths(commandLineArgs.inputPaths)
                    .build();
        } catch (IllegalArgumentException ex) {
            logger.error("HDFS High availability name node configuration is incomplete."
                    + " Configure --namenode-1, --namenode-2 and --namenode-ha");
            System.exit(1);
            return;
        } catch (UncheckedIOException ex) {
            logger.error("Failed to create temporary directory " + commandLineArgs.tmpDir);
            System.exit(1);
            return;
        } catch (IOException ex) {
            logger.error("Failed to initialize plugins", ex);
            System.exit(1);
            return;
        } catch (ClassCastException e) {
            logger.error("Cannot find factory", e);
            System.exit(1);
            return;
        }

        application.start();
    }

    @Override
    public FileCacheStore newFileCacheStore(Accountant accountant) throws IOException {
        return new FileCacheStore(this, accountant);
    }

    @Override
    public RecordPathFactory getPathFactory() {
        return pathFactory;
    }

    @Override
    public StorageDriver getStorageDriver() {
        return storageDriver;
    }

    @Override
    public Compression getCompression() {
        return compression;
    }

    @Override
    public RecordConverterFactory getRecordConverter() {
        return converterFactory;
    }

    @Override
    public RestructureSettings getSettings() {
        return settings;
    }

    @Override
    public HdfsSettings getHdfsSettings() {
        return hdfsSettings;
    }

    public void start() {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism",
                String.valueOf(settings.getNumThreads() - 1));

        Instant timeStart = Instant.now();
        RadarHdfsRestructure hdfsReader = new RadarHdfsRestructure(this);
        try {
            Files.createDirectories(settings.getTempDir());
            for (String input : inputPaths) {
                logger.info("In:  {}", input);
                logger.info("Out: {}", pathFactory.getRoot());
                hdfsReader.start(input);
            }
        } catch (IOException ex) {
            logger.error("Processing failed", ex);
        }

        logger.info("Processed {} files and {} records",
                hdfsReader.getProcessedFileCount(), hdfsReader.getProcessedRecordsCount());
        logger.info("Time taken: {}", formatTime(Duration.between(timeStart, Instant.now())));
    }

    public static class Builder {
        private final RestructureSettings settings;
        private HdfsSettings hdfsSettings;
        private StorageDriver storageDriver;
        private RecordPathFactory pathFactory;
        private CompressionFactory compressionFactory;
        private FormatFactory formatFactory;
        private Map<String, String> properties = new HashMap<>();
        private List<String> inputPaths;

        public Builder(RestructureSettings settings) {
            this.settings = settings;
        }

        public Builder hdfsSettings(HdfsSettings hdfsSettings) {
            this.hdfsSettings = hdfsSettings;
            return this;
        }

        public Builder storageDriver(Object storageDriver) throws ClassCastException {
            this.storageDriver = (StorageDriver) storageDriver;
            return this;
        }

        public Builder pathFactory(Object factory)
                throws ClassCastException {
            this.pathFactory = (RecordPathFactory) factory;
            return this;
        }

        public Builder compressionFactory(Object factory) throws ClassCastException {
            this.compressionFactory = (CompressionFactory) factory;
            return this;
        }

        public Builder formatFactory(Object factory) throws ClassCastException {
            this.formatFactory = (FormatFactory) factory;
            return this;
        }

        public Builder properties(Map<String, String> props) {
            this.properties.putAll(props);
            return this;
        }

        public Application build() throws IOException {
            pathFactory = nonNullOrDefault(pathFactory, ObservationKeyPathFactory::new);
            pathFactory.init(properties);
            storageDriver = nonNullOrDefault(storageDriver, LocalStorageDriver::new);
            storageDriver.init(properties);
            compressionFactory = nonNullOrDefault(compressionFactory, CompressionFactory::new);
            compressionFactory.init(properties);
            formatFactory = nonNullOrDefault(formatFactory, FormatFactory::new);
            formatFactory.init(properties);

            return new Application(this);
        }

        public Builder inputPaths(List<String> inputPaths) {
            this.inputPaths = inputPaths;
            return this;
        }
    }
}
