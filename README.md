# Restructure HDFS files

Build jar from source with
```
./gradlew fatJar
```
and find the output JAR file as `build/libs/restructurehdfs-all-0.1-SNAPSHOT.jar`. Then run with:
```
hadoop jar restructurehdfs-all-0.1.0.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```
