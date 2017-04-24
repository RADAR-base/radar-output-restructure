# Restructure HDFS files

Build jar from source with

```shell
./gradlew build
```
and find the output JAR file as `build/libs/restructurehdfs-all-0.1-SNAPSHOT.jar`. Then run with:

```shell
java -jar restructurehdfs-all-0.1-SNAPSHOT.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```

By default, this will output the data in CSV format. If JSON format is preferred, use the following instead:
```
java -Dorg.radarcns.format=json -jar restructurehdfs-all-0.1-SNAPSHOT.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```
