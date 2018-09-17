package com.jx.stream.builder;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka消费者client
 * 
 * @author rui
 *
 */
public class ConsumerBuilder {
	/**
	 * 服务连接
	 */
	private String bootstrapServers = "192.168.20.243:9092";
	/**
	 * 组
	 */
	private String groupId;
	/**
	 * 自动提交偏移 true false
	 */
	private String enableAutoCommit = "true";
	/**
	 * 设置`enable.auto.commit`,偏移量由`auto.commit.interval.ms`控制自动提交的频率。
	 */
	private String autoCommitIntervalMs = "1000";
	/**
	 * 设置如何把byte转成object类型
	 */
	private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
	/**
	 * 设置如何把byte转成object类型
	 * 
	 * @default org.apache.kafka.common.serialization.StringDeserializer
	 */
	private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

	/**
	 * broker通过心跳机器自动检测test组中失败的进程，消费者会自动ping集群，告诉进群它还活着。只要消费者能够做到这一点，它就被认为是活着的，
	 * 并保留分配给它分区的权利，如果它停止心跳的时间超过session.timeout.ms,那么就会认为是故障的，它的分区将被分配到别的进程
	 */
	private String sessionTimeoutMs;

	/**
	 * 构造消费者客户端
	 * 
	 * @return
	 */
	public <K, V> KafkaConsumer<K, V> kafkaConsumerBuilder() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.bootstrapServers);
		if (null != this.groupId && !"".equals(this.groupId)) {
			props.put("group.id", this.groupId);
		}
		props.put("enable.auto.commit", this.enableAutoCommit);
		props.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
		props.put("key.deserializer", this.keyDeserializer);
		props.put("value.deserializer", this.valueDeserializer);
		return new KafkaConsumer<>(props);
	}

	/**
	 * 构造消费者客户端
	 * 
	 * @param props,以传进来的为准
	 * @return
	 */
	public <K, V> KafkaConsumer<K, V> kafkaConsumerBuilder(Properties props) {
		if (props == null || props.size() <= 0) {
			return kafkaConsumerBuilder();
		}
		return new KafkaConsumer<>(props);
	}

	public ConsumerBuilder build() {
		return this;
	}

	public ConsumerBuilder bootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	public ConsumerBuilder groupId(String groupId) {
		this.groupId = groupId;
		return this;
	}

	public ConsumerBuilder enableAutoCommit(String enableAutoCommit) {
		this.enableAutoCommit = enableAutoCommit;
		return this;
	}

	public ConsumerBuilder autoCommitIntervalMs(String autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
		return this;
	}

	public ConsumerBuilder keyDeserializer(String keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
		return this;
	}

	public ConsumerBuilder valueDeserializer(String valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
		return this;
	}

	public ConsumerBuilder sessionTimeoutMs(String sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
		return this;
	}

	@Override
	public String toString() {
		return "ConsumerBuilder [bootstrapServers=" + bootstrapServers + ", groupId=" + groupId + ", enableAutoCommit="
				+ enableAutoCommit + ", autoCommitIntervalMs=" + autoCommitIntervalMs + ", keyDeserializer="
				+ keyDeserializer + ", valueDeserializer=" + valueDeserializer + ", sessionTimeoutMs="
				+ sessionTimeoutMs + "]";
	}

}
