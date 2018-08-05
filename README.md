# Flume metrics collector
I didn't find Apache Flume plugin for [Telegraf](https://github.com/influxdata/telegraf), so I found that it's nice to have one.

## Test it.
Download Telegraf if you don't have it, then copy flume.go to the following path:
```
cp -a flume $GOPATH/src/github.com/influxdata/telegraf/plugins/inputs
```
Then compile Telegraf, and add this example conf to Telegraf config dir:
```
cat << EOF > /etc/telegraf/telegraf.d/flume.conf
[[inputs.flume]]
  name = "flume_agents_test"
  servers = [
    "http://localhost:41414/flume01.json",
    "http://localhost:41414/flume02.json",
  ]
EOF
```

Finally, run a simple http server with samples data:
```
cd samples
python -m SimpleHTTPServer 41414
# or
# python3 -m http.server 41414
```

## Output example
Based on the samples, the output should be as the following:
```
flume_agents_test,type=SOURCE,name=kafka-source01 AppendBatchAcceptedCount=0,AppendReceivedCount=0,KafkaEmptyCount=0,OpenConnectionCount=0,StartTime=1516706313621,StopTime=0,AppendAcceptedCount=0,AppendBatchReceivedCount=0,EventAcceptedCount=73353071,EventReceivedCount=73353071,KafkaCommitTimer=6432357,KafkaEventGetTimer=616114808
flume_agents_test,type=CHANNEL,name=hdfs-channel02 EventPutAttemptCount=4234929,EventPutSuccessCount=4234929
flume_agents_test,type=CHANNEL,name=hdfs-channel04 EventPutAttemptCount=12767892,EventPutSuccessCount=12767892
flume_agents_test,type=CHANNEL,name=null-channel EventPutAttemptCount=17057471,EventPutSuccessCount=17057471
flume_agents_test,name=hdfs-sink02,type=SINK BatchUnderflowCount=19281,ConnectionClosedCount=704,ConnectionCreatedCount=705,ConnectionFailedCount=0,EventDrainAttemptCount=4234929,EventDrainSuccessCount=4233252,BatchCompleteCount=691,BatchEmptyCount=4573,StartTime=1516706291441,StopTime=0
flume_agents_test,type=SINK,name=hdfs-sink04 BatchEmptyCount=36,ConnectionClosedCount=704,ConnectionCreatedCount=707,ConnectionFailedCount=8,StartTime=1516706291445,StopTime=0,BatchCompleteCount=2276,BatchUnderflowCount=2694,EventDrainAttemptCount=12770801,EventDrainSuccessCount=12764502
flume_agents_test,type=CHANNEL,name=hdfs-channel01 EventPutAttemptCount=2757422,EventPutSuccessCount=2757422
flume_agents_test,type=CHANNEL,name=hdfs-channel03 EventPutSuccessCount=36535357,EventPutAttemptCount=36535357
flume_agents_test,type=SINK,name=hdfs-sink01 EventDrainAttemptCount=2757422,EventDrainSuccessCount=2756217,StopTime=0,BatchEmptyCount=9466,ConnectionClosedCount=704,ConnectionCreatedCount=705,StartTime=1516706291440,BatchCompleteCount=317,BatchUnderflowCount=22371,ConnectionFailedCount=0
flume_agents_test,type=SINK,name=hdfs-sink03 EventDrainSuccessCount=36533183,StartTime=1516706291444,BatchUnderflowCount=3138,ConnectionClosedCount=704,ConnectionCreatedCount=706,EventDrainAttemptCount=36536896,BatchCompleteCount=7147,BatchEmptyCount=124,ConnectionFailedCount=4,StopTime=0
flume_agents_test,type=CHANNEL,name=memChannel EventPutSuccessCount=22948908,EventPutAttemptCount=22948908
flume_agents_test,type=CHANNEL,name=fileChannel EventPutSuccessCount=468085,EventPutAttemptCount=468086
```
