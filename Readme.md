```
scp src/main/org.radarcns.restructureAvroSensorTopics.java maxim@radar-test.thehyve.net:/home/maxim/hadoop-test/

javac -classpath "/opt/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/opt/hadoop/share/hadoop/avro-tools/avro-tools-1.7.7.jar" -d wordcount_classes org.radarcns.restructureAvroSensorTopics.java


jar -cvf restructureAvroSensorTopics.jar -C wordcount_classes/ .

hadoop jar restructureAvroSensorTopics.jar org.radarcns.org.radarcns.restructureAvroSensorTopics
```