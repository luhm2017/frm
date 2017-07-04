#################################direct api##############################################################################################################

--------------------生产环境------------------------------------------
nohup spark-submit --master spark://datacenter17:7077,datacenter18:7077 --class com.lakala.finance.risk_officer.screen.streaming.CreditFundDirectStream --total-executor-cores 6 --conf spark.driver.memory=2g  --conf spark.executor.cores=2 --executor-memory 6g ~/taskJar/FRM-streaming-screen20170613.jar prod logCollect_cleanData credit_fund 1000  hdfs://ns1/spark/checkpoint/CreditLoanDirectStream > ~/log/DisplayDirectStreaming0613  &

#手动恢复命令
cd ~/taskJar/&&java -cp spark-assembly-1.6.3-hadoop2.6.0.jar:screen-display_1.8_1.6.3.jar com.lakala.finance.risk_officer.screen.recov.RecovData prod daily


-----------------生产测试------------------------
#生产执行命令
nohup spark-submit --master spark://datacenter17:7077,datacenter18:7077 --conf spark.driver.memory=2g --class com.lakala.finance.stream.screen.DisplayDirectStreaming --total-executor-cores 6  --conf spark.executor.cores=2 --executor-memory 6g ~/taskJar/FRM-streaming-screen20170616.jar prod_test logCollect_cleanData credit_fund_test 1000  hdfs://ns1/spark/checkpoint/DisplayDirectStreaming_test> ~/log/DisplayDirectStreaming_test0616  &

#手动恢复命令
cd ~/taskJar/&&java -cp spark-assembly-1.6.3-hadoop2.6.0.jar:FRM-streaming-screen20170613.jar com.lakala.finance.stream.screen.recov.RecovData prod_test > recov.log

拿redis
cd ~/taskJar/&&java -cp spark-assembly-1.6.3-hadoop2.6.0.jar:screen-display20170519.jar com.lakala.finance.risk_officer.screen.tools.GetRedisInfo prod_test  dataPlatform.water_pointer_test set

#使用--jars 传递多个jar包
spark-shell --master spark://datacenter17:7077,datacenter18:7077 --total-executor-cores 24  --conf spark.executor.cores=8 --executor-memory 10g --jars $(echo /home/hadoop/taskJar/hbaselib/dependency/*.jar| tr ' ' ',')

#生产kakfa常用命令
cd ~/kafka_2.10-0.8.2.1&&bin/kafka-console-consumer.sh --zookeeper datacenter5:2181,datacenter6:2181,datacenter7:2181 --topic logCollect_cleanData >> kafkamsg.log

