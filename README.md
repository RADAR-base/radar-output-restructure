# Restructure HDFS files

Build jar from source with
```
./gradlew fatJar
```
and find the output JAR file as `build/libs/restructurehdfs-all-0.1-SNAPSHOT.jar`. Then run with:
```
java -jar restructurehdfs-all-0.1-SNAPSHOT.jar <webhdfs_url> <hdfs_topic_path> <output_folder>
```
