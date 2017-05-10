# Restructure HDFS files

[![Build Status](https://travis-ci.org/RADAR-CNS/Restructure-HDFS-topic.svg?branch=master)](https://travis-ci.org/RADAR-CNS/Restructure-HDFS-topic)

Data streamed to HDFS using the [RADAR HDFS sink connector](https://github.com/RADAR-CNS/RADAR-HDFS-Sink-Connector) is streamed to files based on sensor only. This package can transform that output to a local directory structure as follows: `userId/topic/date_hour.csv`. The date and hour is extracted from the `time` field of each record, and is formatted in UTC time.

## Usage

This package is included in the [RADAR-Docker](https://github.com/RADAR-CNS/RADAR-Docker) repository, in the `dcompose/radar-cp-hadoop-stack/hdfs_restructure.sh` script.

## Advanced usage

Build jar from source with

```shell
./gradlew build
```
and find the output JAR file as `build/libs/restructurehdfs-all-0.1.1.jar`. Then run with:

```shell
java -jar restructurehdfs-all-0.1.1-SNAPSHOT.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```

By default, this will output the data in CSV format. If JSON format is preferred, use the following instead:
```
java -Dorg.radarcns.format=json -jar restructurehdfs-all-0.1.1.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```
