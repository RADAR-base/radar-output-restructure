# Restructure Kafka connector output files

Data streamed by a Kafka Connector will be converted to a RADAR-base oriented output directory, by organizing it by project, user and collection date.
It supports data written by [RADAR S3 sink connector](https://github.com/RADAR-base/RADAR-S3-Connector) is streamed to files based on topic name only. This package transforms that output to a local directory structure as follows: `projectId/userId/topic/date_hour.csv`. The date and hour are extracted from the `time` field of each record, and is formatted in UTC time.

## Upgrade instructions

Since version 2.0.0, HDFS is no longer supported, only AWS S3 or Azure Blob Storage, and local file system compatible. If HDFS is still needed, please implement a HDFS source storage factory with constructor `org.radarbase.output.source.HdfsSourceStorageFactory(resourceConfig: ResourceConfig, tempPath: Path)` with method `createSourceStorage(): SourceStorage`. This implementation may be added as a separate JAR in the `lib/radar-output-plugins/` directory of where the distribution is installed.

When upgrading to version 1.2.0, please follow the following instructions:

  - When using local target storage, ensure that:
    1. it is writable by the user 101, or change the runtime user using the docker command-line flag `--user` to a user that can write to the target storage and
    2. local storage properties `userId` and `groupId` are set to values that can write to the target storage.

When upgrading to version 1.0.0 or later from version 0.6.0 please follow the following instructions:

  - This package now relies on Redis for locking and offset management. Please install Redis or use
    the docker-compose.yml file to start it.
  - Write configuration file `restructure.yml` to match settings used with 0.6.0
    - HDFS settings have moved to `source`. Specify all name nodes in the `nameNodes`
      property. The `name` property is no longer used.

      ```yaml
      source:
        type: hdfs
        hdfs:
          nameNodes: [hdfs-namenode]
      ```
    - Add a `redis` block:

      ```yaml
      redis:
        uri: redis://localhost:6379
      ```
    - Offset accounting will automatically be migrated from a file-based storage to a Redis entry
      as radar-output processes the topic. Please do not remove the offsets directory until it is
      empty.
    - storage settings have moved to the `target` block. Using local output directory:

      ```yaml
      target:
        type: local
        local:
          # User ID to write data as. This only works when explicitly setting
          # the runtime user to root.
          userId: 123
          # Group ID to write data as. This only works when explicitly setting
          # the runtime user to root.
          groupId: 123
      ```

      With the `S3StorageDriver`, use the following configuration instead:
      ```yaml
      target:
        type: s3
        s3:
          endpoint: https://my-region.s3.aws.amazon.com  # or http://localhost:9000 for local minio
          accessToken: ABA...
          secretKey: CSD...
          bucket: myBucketName
      ```

When upgrading to version 0.6.0 from version 0.5.x or earlier, please follow the following instructions:
  - Write configuration file `restructure.yml` to match command-line settings used with 0.5.x.
  - If needed, move all entries of `offsets.csv` to their per-topic file in `offsets/<topic>.csv`. First go to the output directory, then run the `bin/migrate-offsets-to-0.6.0.sh` script.

## Docker usage

This package is available as docker image [`radarbase/radar-output-restructure`](https://hub.docker.com/r/radarbase/radar-output-restructure). The entrypoint of the image is the current application. So in all the commands listed in usage, replace `radar-output-restructure` with for example:

```shell
docker run --rm -t --network s3 -v "$PWD/output:/output" radarbase/radar-output-restructure:2.3.2 -o /output /myTopic
```

## Command line usage

To display the usage and all available options you can use the help option as follows:
```shell
radar-output-restructure --help
```
Note that the options preceded by the `*` in the above output are required to run the app. Also note that there can be multiple input paths from which to read the files. Eg - `/topicAndroidNew/topic1 /topicAndroidNew/topic2 ...`. Provide at least one input path.

Each argument, as well as much more, can be supplied in a config file. The default name of the config file is `restructure.yml`. Please refer to `restructure.yml` in the current directory for all available options. An alternative file can be specified with the `-F` flag.

### File Format

By default, this will output the data in CSV format. If JSON format is preferred, use the following instead:
```shell
radar-output-restructure --format json --output-directory <output_folder>  <input_path_1> [<input_path_2> ...]
```

By default, files records are not deduplicated after writing. To enable this behaviour, specify the option `--deduplicate` or `-d`. This set to false by default because of an issue with Biovotion data. Please see - [issue #16](https://github.com/RADAR-base/radar-output-restructure/issues/16) before enabling it. Deduplication can also be enabled or disabled per topic using the config file. If lines should be deduplicated using a subset of fields, e.g. only `sourceId` and `time` define a unique record and only the last record with duplicate values should be kept, then specify `topics: <topicName>: deduplication: distinctFields: [key.sourceId, value.time]`.

### Compression

Another option is to output the data in compressed form. All files will get the `gz` suffix, and can be decompressed with a GZIP decoder. Note that for a very small number of records, this may actually increase the file size. Zip compression is also available.
```
radar-output-restructure --compression gzip --output-directory <output_folder> <input_path_1> [<input_path_2> ...]
```

### Redis

This package assumes a Redis service running. See the example `restructure.yml` for configuration options.

### Source and target

The `source` and `target` properties contain resource descriptions. The source can have two types, `azure` and `s3`:

```yaml
source:
  type: s3  # azure or s3
  s3:
    endpoint: http://localhost:9000  # using AWS S3 endpoint is also possible.
    bucket: radar
    accessToken: minioadmin
    secretKey: minioadmin
  # only actually needed if source type is hdfs
  azure:
    # azure options
  index:
    # Interval to fully synchronize the index with the source storage
    fullSyncInterval: 3600
    # Interval to sync empty directories with.
    # They are also synced during a full sync.
    emptyDirectorySyncInterval: 900
```

The index makes a scan of the source before any operations. Further list operations are done on the index only. This is especially relevant for S3 storage where list operations are priced.

The target is similar, and in addition supports the local file system (`local`).

```yaml
target:
  type: s3  # s3, local or azure
  s3:
    endpoint: http://localhost:9000
    bucket: out
    accessToken: minioadmin
    secretKey: minioadmin
  # only actually needed if target type is local
  local:
    userId: 1000  # write as regular user, use -1 to use current user (default).
    groupId: 100  # write as regular group, use -1 to use current user (default).
```

Secrets can be provided as environment variables as well:

| Environment variable        | Corresponding value        |
|-----------------------------|----------------------------|
| `SOURCE_S3_ACCESS_TOKEN`    | `source.s3.accessToken`    |
| `SOURCE_S3_SECRET_KEY`      | `source.s3.secretKey`      |
| `SOURCE_AZURE_USERNAME`     | `source.azure.username`    |
| `SOURCE_AZURE_PASSWORD`     | `source.azure.password`    |
| `SOURCE_AZURE_ACCOUNT_NAME` | `source.azure.accountName` |
| `SOURCE_AZURE_ACCOUNT_KEY`  | `source.azure.accountKey`  |
| `SOURCE_AZURE_SAS_TOKEN`    | `source.azure.sasToken`    |
| `REDIS_URL`                 | `redis.url`                |

Replace `SOURCE` with `TARGET` in the variables above to configure the target storage.

### Path format

The output path at the target storage is determined by the path format. The class that handles path
output by default is the `org.radarbase.output.path.FormattedPathFactory`. The default format is
```
${projectId}/${userId}/${topic}/${filename}
```
Each format parameter is enclosed by a dollar sign with curly brackets.

The full set of parameters is listed here:
```yaml
paths:
  # Input directories in source storage
  inputs:
    - /testIn
  # Temporary directory for local file processing.
  temp: ./output/+tmp
  # Output directory in target storage
  output: /output
  # Output path construction factory
  factory: org.radarbase.output.path.FormattedPathFactory
  # Additional properties
  # properties:
  #   format: ${projectId}/${userId}/${topic}/${time:mm}/${time:YYYYmmDD_HH'00'}${attempt}${extension}
  #   plugins: fixed time key value org.example.plugin.MyPathPlugin
```

The FormattedPathFactory can use multiple plugins to format paths based on a given record.
The `fixed` plugin has a number of fixed parameters that can be used:

| Parameter | Description                                                             |
|-----------|-------------------------------------------------------------------------|
| projectId | record project ID                                                       |
| userId    | record user ID                                                          |
| sourceId  | record source ID                                                        |
| topic     | Kafka topic                                                             | 
| filename  | default time binning with attempt suffix and file extension             |
| attempt   | attempt suffix for if a file with an incompatible format already exists |
| extension | file extension                                                          |

At least `filename` should be used, or a combination of `attempt` and `extension`.

Then there are also plugins that take their own format. The `time` plugin formats a parameter according to the record time. It takes parameters with format `time:<date format>` where `<date format>` should be replaced by a [Java date format](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html), such as `YYYY-mm-dd`. The plugin tries to use the following time fields, in this order: a double `time` in the value struct, `timeStart` double or `start` long in the key struct, `dateTime` string in the value struct, `date` string in the value struct, `timeReceived` double in the value struct or `timeCompleted` double in the value struct. The first valid value used. If no valid time values are found, `unknown-date` is returned.

The `key` and `value` plugins read values from the key or value structs of a given record. For example, parameter `value:color.red` will attempt to read the value struct, finding first the `color` field and then the enclosed `red` field. If no such value exists, `unknown-value` will be used in the format.

### Cleaner

Source files can be automatically be removed by a cleaner process. This checks whether the file has already been extracted and is older than a configured age. This feature is not enabled by default. It can be configured in the `cleaner` configuration section:

```yaml
cleaner:
  # Enable cleaning up old source files
  enable: true
  # Interval in seconds to clean data
  interval: 1260  # 21 minutes
  # Number of days after which a source file is considered old
  age: 7
```

The cleaner can also be enabled with the `--cleaner` command-line flag. To run the cleaner as a separate process from output restructuring, start a process that has configuration property `worker: enable: false` or command-line argument `--no-restructure`.

### Service

To run the output generator as a service that will regularly poll the source directory, add the `--service` flag and optionally the `--interval` flag to adjust the polling interval or use the corresponding configuration file parameters.

## Local build

This package requires at least Java JDK 8. Build the distribution with

```shell
./gradlew build
```

and install the package into `/usr/local` with for example
```shell
sudo mkdir -p /usr/local
sudo tar -xzf build/distributions/radar-output-restructure-2.3.2.tar.gz -C /usr/local --strip-components=1
```

Now the `radar-output-restructure` command should be available.

### Extending the connector

To implement alternative storage paths, storage drivers or storage formats, put your custom JAR in
`$APP_DIR/lib/radar-output-plugins`. To load them, use the following options:

| Parameter                   | Base class                                            | Behaviour                                  | Default              |
|-----------------------------|-------------------------------------------------------|--------------------------------------------|----------------------|
| `paths: factory: ...`       | `org.radarbase.output.path.RecordPathFactory`         | Factory to create output path names with.  | FormattedPathFactory |
| `format: factory: ...`      | `org.radarbase.output.format.FormatFactory`           | Factory for output formats.                | FormatFactory        |
| `compression: factory: ...` | `org.radarbase.output.compression.CompressionFactory` | Factory class to use for data compression. | CompressionFactory   |

The respective `<type>: properties: {}` configuration parameters can be used to provide custom configuration of the factory. This configuration will be passed to the `Plugin#init(Map<String, String>)` method.

By adding additional path format plugins to the classpath, the path format of FormattedPathFactory may be expanded with different parameters or lookup engines.
