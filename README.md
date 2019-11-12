# Restructure HDFS files

[![Build Status](https://travis-ci.org/RADAR-base/Restructure-HDFS-topic.svg?branch=master)](https://travis-ci.org/RADAR-base/Restructure-HDFS-topic)

Data streamed to HDFS using the [RADAR HDFS sink connector](https://github.com/RADAR-base/RADAR-HDFS-Sink-Connector) is streamed to files based on sensor only. This package can transform that output to a local directory structure as follows: `userId/topic/date_hour.csv`. The date and hour is extracted from the `time` field of each record, and is formatted in UTC time. This package is included in the [RADAR-Docker](https://github.com/RADAR-base/RADAR-Docker) repository, in the `dcompose/radar-cp-hadoop-stack/hdfs_restructure.sh` script.

## Docker usage

This package is available as docker image [`radarbase/radar-hdfs-restructure`](https://hub.docker.com/r/radarbase/radar-hdfs-restructure). The entrypoint of the image is the current application. So in all of the commands listed in usage, replace `radar-hdfs-restructure` with for example:
```shell
docker run --rm -t --network hadoop -v "$PWD/output:/output" radarbase/radar-hdfs-restructure:0.5.7 -n hdfs-namenode -o /output /myTopic
```
if your docker cluster is running in the `hadoop` network and your output directory should be `./output`.

## Command line usage

When the application is installed, it can be used as follows:

```shell
radar-hdfs-restructure --nameservice <hdfs_node> --output-directory <output_folder> <input_path_1> [<input_path_2> ...]
```
or you can use the short form as well like - 
```shell
radar-hdfs-restructure -n <hdfs_node> -o <output_folder> <input_path_1> [<input_path_2> ...]
```

To display the usage and all available options you can use the help option as follows - 
```shell
radar-hdfs-restructure --help
```
Note that the options preceded by the `*` in the above output are required to run the app. Also note that there can be multiple input paths from which to read the files. Eg - `/topicAndroidNew/topic1 /topicAndroidNew/topic2 ...`. At least one input path is required.

Each argument, as well as much more, can be supplied in a config file. The default name of the config file is `restructure.yml`. Please refer to `restructure.yml` in the current directory for all available options. An alternative file can be specified with the `-F` flag.

### File Format

By default, this will output the data in CSV format. If JSON format is preferred, use the following instead:
```shell
radar-hdfs-restructure --format json --nameservice <hdfs_node> --output-directory <output_folder>  <input_path_1> [<input_path_2> ...]
```

By default, files records are not deduplicated after writing. To enable this behaviour, specify the option `--deduplicate` or `-d`. This set to false by default because of an issue with Biovotion data. Please see - [issue #16](https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16) before enabling it. Deduplication can also be enabled or disabled per topic using the config file. If lines should be deduplicated using a subset of fields, e.g. only `sourceId` and `time` define a unique record and only the last record with duplicate values should be kept, then specify `topics: <topicName>: deduplicateFields: [sourceId, time]`.

### Compression

Another option is to output the data in compressed form. All files will get the `gz` suffix, and can be decompressed with a GZIP decoder. Note that for a very small number of records, this may actually increase the file size.
```
radar-hdfs-restructure --compression gzip  --nameservice <hdfs_node> --output-directory <output_folder> <input_path_1> [<input_path_2> ...]
```

### Storage

When using local storage, to set the output user ID and group ID, specify the `-p local-uid=123` and `-p local-gid=12` properties.

### Service

To run the output generator as a service that will regularly poll the HDFS directory, add the `--service` flag and optionally the `--interval` flag to adjust the polling interval or use the corresponding configuration file parameters.

## Local build

This package requires at least Java JDK 8. Build the distribution with

```shell
./gradlew build
```

and install the package into `/usr/local` with for example
```shell
sudo mkdir -p /usr/local
sudo tar -xzf build/distributions/radar-hdfs-restructure-0.5.7.tar.gz -C /usr/local --strip-components=1
```

Now the `radar-hdfs-restructure` command should be available.

### Extending the connector

To implement alternative storage paths, storage drivers or storage formats, put your custom JAR in
`$APP_DIR/lib/radar-hdfs-plugins`. To load them, use the following options:

| Parameter                   | Base class                                          | Behaviour                                  | Default                   |
| --------------------------- | --------------------------------------------------- | ------------------------------------------ | ------------------------- |
| `paths: factory: ...`       | `org.radarbase.hdfs.path.RecordPathFactory`         | Factory to create output path names with.  | ObservationKeyPathFactory |
| `storage: factory: ...`     | `org.radarbase.hdfs.storage.StorageDriver`          | Storage driver to use for storing data.    | LocalStorageDriver        |
| `format: factory: ...`      | `org.radarbase.hdfs.format.FormatFactory`           | Factory for output formats.                | FormatFactory             |
| `compression: factory: ...` | `org.radarbase.hdfs.compression.CompressionFactory` | Factory class to use for data compression. | CompressionFactory        |

The respective `<type>: properties: {}` configuration parameters can be used to provide custom configuration of the factory. This configuration will be passed to the `Plugin#init(Map<String, String>)` method.
