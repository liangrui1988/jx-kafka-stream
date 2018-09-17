package com.jx.stream.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaClientUtils {

	private static String bootstrap_servers = "192.168.20.243:9092";

	/**
	 * 获取生产Producer
	 * 
	 * @return
	 */

	public static <K, V> Producer<K, V> getProducer() {
		Producer<K, V> producer = new KafkaProducer<>(getDefultProperties());
		return producer;
	}

	/**
	 * 获取生产Producer
	 * 
	 * @param properties
	 * @return
	 */
	public static <K, V> Producer<K, V> getProducer(Properties properties) {
		if (properties == null) {
			return getProducer();
		}
		Producer<K, V> producer = new KafkaProducer<>(getDefultProperties());
		return producer;
	}

	/**
	 * 默认属性
	 * 
	 * @return
	 */
	public static Properties getDefultProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap_servers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	/**
	 * 默认属性
	 * 
	 * @return
	 */
	public static Properties getDefultConsumerProperties(String groupId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap_servers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");// 自动偏移增长
		props.put("auto.commit.interval.ms", "1000");// 设置`enable.auto.commit`,偏移量由`auto.commit.interval.ms`控制自动提交的频率。
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

}
