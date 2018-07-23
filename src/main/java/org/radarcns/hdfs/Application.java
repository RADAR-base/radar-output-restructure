package org.radarcns.hdfs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
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

import java.io.IOException;
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

        RestructureSettings settings = new RestructureSettings.Builder(commandLineArgs.outputDirectory)
                .compression(commandLineArgs.compression)
                .cacheSize(commandLineArgs.cacheSize)
                .format(commandLineArgs.format)
                .doDeduplicate(commandLineArgs.deduplicate)
                .tempDir(commandLineArgs.tmpDir)
                .numThreads(commandLineArgs.numThreads)
                .build();

        Application application;

        try {
            HdfsSettings hdfsSettings = new HdfsSettings.Builder(commandLineArgs.hdfsName)
                    .hdfsHighAvailability(commandLineArgs.hdfsHa,
                            commandLineArgs.hdfsUri1, commandLineArgs.hdfsUri2)
                    .build();

            application = new Builder(settings, hdfsSettings)
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
        Instant timeStart = Instant.now();
        RadarHdfsRestructure hdfsReader = new RadarHdfsRestructure(this);
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
        logger.info("{}", Timer.getInstance());
    }

    public static class Builder {
        private final RestructureSettings settings;
        private final HdfsSettings hdfsSettings;
        private StorageDriver storageDriver;
        private RecordPathFactory pathFactory;
        private CompressionFactory compressionFactory;
        private FormatFactory formatFactory;
        private Map<String, String> properties = new HashMap<>();
        private List<String> inputPaths;

        public Builder(RestructureSettings settings, HdfsSettings hdfsSettings) {
            this.settings = settings;
            this.hdfsSettings = hdfsSettings;
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
