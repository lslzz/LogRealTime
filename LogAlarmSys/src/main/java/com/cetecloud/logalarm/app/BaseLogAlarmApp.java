package com.cetecloud.logalarm.app;

import com.alibaba.fastjson.JSONObject;
import com.cetecloud.logalarm.app.func.DimSink;
import com.cetecloud.logalarm.app.func.MySourceFunction;
import com.cetecloud.logalarm.utils.MyKafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class BaseLogAlarmApp {
    public static void main(String[] args) throws Exception {
        // 1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2 设置检查点
       /* env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.1 设置超时时间
        checkpointConfig.setCheckpointTimeout(TimeUnit.SECONDS.toMillis(10));
        // 2.2 设置失败次数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        // 2.3 设置最小等待间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(3));
        // 2.5 设置保留checkpoint
        // checkpoint是给flink程序在重试的时候用的，不是给用户使用的，用户使用的是savepoint
        // 如果想要checkpoint被我们利用起来，设置为 手动取消job后，仍然保存checkpoint
        checkpointConfig.
                enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.6 指定状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall/flink"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/
        // 3 读取kafka数据
        String hadoopTopic = "hadoop_log";
        String kafkaTopic = "kafka_log";
        String groupId = "group_BaseLogAlarmApp";

        FlinkKafkaConsumer<String> hadoopFunction = MyKafkaUtils.getKafkaSourceFunction(hadoopTopic, groupId);
        hadoopFunction.setCommitOffsetsOnCheckpoints(true);

        FlinkKafkaConsumer<String> kafkaFunction = MyKafkaUtils.getKafkaSourceFunction(kafkaTopic, groupId);
        kafkaFunction.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> hadoopDS = env.addSource(hadoopFunction);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaFunction);

        SingleOutputStreamOperator<JSONObject> hadoopJsonObjDS = hadoopDS.process(new MySourceFunction(hadoopTopic));
        SingleOutputStreamOperator<JSONObject> kafkaJsonObjDS = kafkaDS.process(new MySourceFunction(kafkaTopic));

        DataStream<JSONObject> unionDS = hadoopJsonObjDS.union(kafkaJsonObjDS);
        // 将原始数据写入Hbase
        unionDS.addSink(new DimSink());
        hadoopJsonObjDS.print("hadoopJsonObjDS");
        kafkaJsonObjDS.print("kafkaJsonObjDS");
        env.execute("BaseLogAlarmApp");
    }
}
