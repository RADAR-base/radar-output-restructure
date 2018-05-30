# Restructure HDFS files

[![Build Status](https://travis-ci.org/RADAR-base/Restructure-HDFS-topic.svg?branch=master)](https://travis-ci.org/RADAR-base/Restructure-HDFS-topic)

Data streamed to HDFS using the [RADAR HDFS sink connector](https://github.com/RADAR-CNS/RADAR-HDFS-Sink-Connector) is streamed to files based on sensor only. This package can transform that output to a local directory structure as follows: `userId/topic/date_hour.csv`. The date and hour is extracted from the `time` field of each record, and is formatted in UTC time.

## Usage

This package is included in the [RADAR-Docker](https://github.com/RADAR-CNS/RADAR-Docker) repository, in the `dcompose/radar-cp-hadoop-stack/hdfs_restructure.sh` script.

## Advanced usage

Build jar from source with

```shell
./gradlew build
```
and find the output JAR file as `build/libs/restructurehdfs-0.3.3-all.jar`. Then run with:

```shell
java -jar restructurehdfs-0.3.3-all.jar --hdfs-uri <webhdfs_url> --hdfs-root-directory <hdfs_topic_path> --output-directory <output_folder>
```
or you can use the short form as well like - 
```shell
java -jar restructurehdfs-0.3.3-all.jar -u <webhdfs_url> -i <hdfs_topic_path> -o <output_folder>
```

To display the usage and all available options you can use the help option as follows - 
```shell
java -jar restructurehdfs-0.3.3-all.jar --help
```
Note that the options preceded by the `*` in the above output are required to run the app.

By default, this will output the data in CSV format. If JSON format is preferred, use the following instead:
```shell
java -jar restructurehdfs-0.3.3-all.jar --format json --hdfs-uri <webhdfs_url> --hdfs-root-directory <hdfs_topic_path> --output-directory <output_folder>
```

Another option is to output the data in compressed form. All files will get the `gz` suffix, and can be decompressed with a GZIP decoder. Note that for a very small number of records, this may actually increase the file size.
```
java -jar restructurehdfs-0.3.3-all.jar --compression gzip  --hdfs-uri <webhdfs_url> --hdfs-root-directory <hdfs_topic_path> --output-directory <output_folder>
```

Finally, by default, files records are not deduplicated after writing. To enable this behaviour, specify the option `--deduplicate` or `-d`. This set to false by default because of an issue with Biovotion data. Please see - [issue #16](https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16) before enabling it.
