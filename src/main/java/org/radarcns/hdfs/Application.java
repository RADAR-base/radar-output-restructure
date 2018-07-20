package org.radarcns.hdfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.radarcns.hdfs.data.Compression;
import org.radarcns.hdfs.data.CompressionFactory;
import org.radarcns.hdfs.data.FileCacheStore;
import org.radarcns.hdfs.data.FormatFactory;
import org.radarcns.hdfs.data.LocalStorageDriver;
import org.radarcns.hdfs.data.RecordConverterFactory;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.commandline.CommandLineArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.radarcns.hdfs.util.ProgressBar.formatTime;
import static org.radarcns.hdfs.util.commandline.CommandLineArgs.instantiate;
import static org.radarcns.hdfs.util.commandline.CommandLineArgs.nonNullOrDefault;

public class Application implements FileStoreFactory {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    public static final int CACHE_SIZE_DEFAULT = 100;

    private final StorageDriver storageDriver;
    private final RecordConverterFactory converterFactory;
    private final Compression compression;
    private final boolean doDeduplicate;
    private final RadarHdfsRestructure hdfsReader;
    private final RecordPathFactory pathFactory;
    private final List<String> inputPaths;
    private final int cacheSize;

    private Application(Builder builder) {
        this.storageDriver = builder.storageDriver;
        this.doDeduplicate = builder.doDeduplicate;

        converterFactory = builder.formatFactory.get(builder.format);
        compression = builder.compressionFactory.get(builder.compression);
        cacheSize = builder.cacheSize;

        pathFactory = builder.pathFactory;
        String extension = converterFactory.getExtension() + compression.getExtension();
        this.pathFactory.setExtension(extension);
        this.pathFactory.setRoot(Paths.get(builder.root.replaceAll("/$", "")));

        this.inputPaths = builder.inputPaths;

        hdfsReader = builder.hdfsReader;
        hdfsReader.setFileStoreFactory(this);
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


        RadarHdfsRestructure restr;

        Application application;

        try {
            restr = new RadarHdfsRestructure.Builder(commandLineArgs.hdfsUri, commandLineArgs.outputDirectory)
                    .hdfsHighAvailability(commandLineArgs.hdfsHa,
                            commandLineArgs.hdfsUri1, commandLineArgs.hdfsUri2)
                    .numThreads(commandLineArgs.numThreads)
                    .build();

            application = new Builder(restr, commandLineArgs.outputDirectory)
                    .compression(commandLineArgs.compression)
                    .doDeduplicate(commandLineArgs.deduplicate)
                    .format(commandLineArgs.format)
                    .pathFactory(commandLineArgs.pathFactory)
                    .compressionFactory(commandLineArgs.compressionFactory)
                    .formatFactory(commandLineArgs.formatFactory)
                    .storageDriver(commandLineArgs.storageDriver)
                    .properties(commandLineArgs.properties)
                    .inputPaths(commandLineArgs.inputPaths)
                    .cacheSize(commandLineArgs.cacheSize)
                    .build();
        } catch (IllegalArgumentException ex) {
            logger.error("HDFS High availability name node configuration is incomplete."
                    + " Configure --namenode-1, --namenode-2 and --namenode-ha");
            System.exit(1);
            return;
        } catch (IOException ex) {
            logger.error("Failed to initialize plugins", ex);
            System.exit(1);
            return;
        } catch (ReflectiveOperationException | ClassCastException e) {
            logger.error("Cannot find factory", e);
            System.exit(1);
            return;
        }

        application.start();
    }

    public FileCacheStore newFileCacheStore() throws IOException {
        return new FileCacheStore(storageDriver, converterFactory, cacheSize, compression,
                doDeduplicate);
    }

    @Override
    public RecordPathFactory getPathFactory() {
        return pathFactory;
    }

    @Override
    public StorageDriver getStorageDriver() {
        return storageDriver;
    }

    public void start() {
        Instant timeStart = Instant.now();

        try {
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
        private final String root;
        private StorageDriver storageDriver;
        private boolean doDeduplicate;
        private String compression;
        private String format;
        private RecordPathFactory pathFactory;
        private CompressionFactory compressionFactory;
        private FormatFactory formatFactory;
        private Map<String, String> properties = new HashMap<>();
        private RadarHdfsRestructure hdfsReader;
        private List<String> inputPaths;
        private int cacheSize = CACHE_SIZE_DEFAULT;

        public Builder(RadarHdfsRestructure restr, String root) {
            this.hdfsReader = restr;
            this.root = root;
        }

        public Builder compression(final String compression) {
            this.compression = compression;
            return this;
        }

        public Builder doDeduplicate(final boolean dedup) {
            this.doDeduplicate = dedup;
            return this;
        }

        public Builder format(final String format) {
            this.format = format;
            return this;
        }

        public Builder storageDriver(String storageDriver)
                throws ReflectiveOperationException, ClassCastException {
            this.storageDriver = instantiate(storageDriver, StorageDriver.class);
            return this;
        }

        public Builder pathFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.pathFactory = instantiate(factoryClassName, RecordPathFactory.class);
            return this;
        }

        public Builder compressionFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.compressionFactory = instantiate(factoryClassName, CompressionFactory.class);
            return this;
        }

        public Builder formatFactory(String factoryClassName)
                throws ReflectiveOperationException, ClassCastException {
            this.formatFactory = instantiate(factoryClassName, FormatFactory.class);
            return this;
        }

        public Builder properties(Map<String, String> props) {
            this.properties.putAll(props);
            return this;
        }

        public Builder cacheSize(Integer size) {
            if (size == null) {
                return this;
            }
            if (size < 1) {
                throw new IllegalArgumentException("Cannot use cache size smaller than 1");
            }
            cacheSize = size;
            return this;
        }

        public Application build() throws IOException {
            compression = nonNullOrDefault(compression, () -> "identity");
            format = nonNullOrDefault(format, () -> "csv");
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
