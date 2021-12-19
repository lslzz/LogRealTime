package com.cetecloud.logalarm.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author lslzz
 */
public class MyKafkaUtils {
    private static final String BOOTSTRAP_SERVER = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
    private static final String DEFAULT_TOPIC = "default_topic";

    /**
     * 获取KafkaSourceFunction
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSourceFunction(String topic,String groupId){
        Properties consumerProp = getConsumerProp(groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),consumerProp);
    }

    /*public static FlinkKafkaProducer<String> getKafkaSinkFunction(String topic) {
        Properties producerProp = getProducerProp();
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }*/

    /**
     * 获取KafkaSinkFunction
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSinkFunction(String topic) {
        Properties producerProp = getProducerProp();

        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic,element.getBytes());
            }
        },producerProp,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取KafkaSinkFunctionBySchema
     * @param kafkaSerializationSchema
     * @param <T> 声明泛型模板
     * @return
     */
    public static <T>FlinkKafkaProducer<T> getKafkaSinkFunctionBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties producerProp = getProducerProp();

        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,producerProp,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获取生产者Kafka的配置信息
     * @return
     */
    private static Properties getProducerProp() {
        Properties producerProp = new Properties();
        producerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");
        return producerProp;
    }

    /**
     * 获取消费者Kafka的配置信息
     * @param groupId
     * @return
     */
    private static Properties getConsumerProp(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return properties;
    }

    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVER + "'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'";
        return ddl;

    }
}
