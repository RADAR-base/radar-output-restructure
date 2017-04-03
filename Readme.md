Run manually with:
```
hadoop jar restructurehdfs-all-0.1.0.jar webhdfs://radar-test.thehyve.net:50070 /topicAndroidPhoneNew/ output2
```

Cron example, executing every hour (/etc/crontab):
```
00 *    * * *   root    hadoop jar /home/maxim/hadoop-test/restructurehdfs-all-0.1.0.jar webhdfs://radar-test.thehyve.net:50070/ /udooE4Time/ /home/maxim/hadoop-test/outputCron > /home/maxim/hadoop-test/logCron.txt
```