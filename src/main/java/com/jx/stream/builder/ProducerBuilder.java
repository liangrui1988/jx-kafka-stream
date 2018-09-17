package com.jx.stream.builder;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * kafka生产者客户端client
 * 
 * @author rui
 *
 */
public class ProducerBuilder {

	/**
	 * 组
	 */
	private String groupId;
	/**
	 * 服务连接
	 */
	private String bootstrapServers = "192.168.20.243:9092";

	/**
	 * 是判别请求是否为完整的条件（就是是判断是不是成功发送了）。我们指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的。
	 */
	private String acks = "all";
	/**
	 * 如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。
	 */
	private Integer retries = 0;
	/**
	 * 
	 * producer(生产者)缓存每个分区未发送消息。缓存的大小是通过 batch.size
	 * 配置指定的。值较大的话将会产生更大的批。并需要更多的内存（因为每个“活跃”的分区都有1个缓冲区）
	 */
	private Integer batchSize = 16384;
	/**
	 * 默认缓冲可立即发送，即遍缓冲空间还没有满，但是，如果你想减少请求的数量，可以设置linger.ms大于0。这将指示生产者发送请求之前等待一段时间，
	 * 希望更多的消息填补到未满的批中。这类似于TCP的算法，例如上面的代码段，可能100条消息在一个请求发送，因为我们设置了linger(逗留)
	 * 时间为1毫秒，然后，如果我们没有填满缓冲区，这个设置将增加1毫秒的延迟请求以等待更多的消息。需要注意的是，在高负载下，相近的时间一般也会组成批，
	 * 即使是 linger.ms=0。在不处于高负载的情况下，如果设置比0大，以少量的延迟代价换取更少的，更有效的请求。
	 */
	private Integer lingerMs = 1;

	/**
	 * 控制生产者可用的缓存总量，如果消息发送速度比其传输到服务器的快，将会耗尽这个缓存空间。当缓存空间耗尽，其他发送调用将被阻塞，
	 * 阻塞时间的阈值通过max.block.ms设定，之后它将抛出一个TimeoutException。
	 */
	private Integer bufferMemory = 33554432;

	/**
	 * 设置如何把byte转成object类型
	 * 
	 * 将用户提供的key和value对象ProducerRecord转换成字节，
	 * 你可以使用附带的ByteArraySerializaer或StringSerializer处理简单的string或byte类型。
	 */
	private String keyDeserializer = "org.apache.kafka.common.serialization.StringSerializer";
	/**
	 * 设置如何把byte转成object类型
	 * 
	 */
	private String valueDeserializer = "org.apache.kafka.common.serialization.StringSerializer";

	/**
	 * 构造生产者客户端
	 * 
	 * @return
	 */
	public <K, V> Producer<K, V> kafkaProducerBuilder() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.bootstrapServers);
		if (null != this.groupId && !"".equals(this.groupId)) {
			props.put("group.id", this.groupId);
		}
		props.put("key.serializer", this.keyDeserializer);
		props.put("value.serializer", this.valueDeserializer);
		props.put("acks", this.acks);
		props.put("retries", this.retries);
		props.put("batch.size", this.batchSize);
		props.put("linger.ms", this.lingerMs);
		props.put("buffer.memory", this.bufferMemory);
		return new KafkaProducer<>(props);
	}

	/**
	 * 构造生产者客户端
	 * 
	 * @param props,以传进来的为准
	 * @return
	 */
	public <K, V> Producer<K, V> kafkaProducerBuilder(Properties props) {
		if (props == null || props.size() <= 0) {
			return kafkaProducerBuilder();
		}
		return new KafkaProducer<>(props);
	}

	public ProducerBuilder build() {
		return this;
	}

	public ProducerBuilder groupId(String groupId) {
		this.groupId = groupId;
		return this;
	}

	public ProducerBuilder bootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	public ProducerBuilder keyDeserializer(String keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
		return this;
	}

	public ProducerBuilder valueDeserializer(String valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
		return this;
	}

	public ProducerBuilder acks(String acks) {
		this.acks = acks;
		return this;
	}

	public ProducerBuilder retries(Integer retries) {
		this.retries = retries;
		return this;
	}

	public ProducerBuilder batchSize(Integer batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public ProducerBuilder lingerMs(Integer lingerMs) {
		this.lingerMs = lingerMs;
		return this;
	}

	public ProducerBuilder setBufferMemory(Integer bufferMemory) {
		this.bufferMemory = bufferMemory;
		return this;
	}

	@Override
	public String toString() {
		return "ProducerBuilder [groupId=" + groupId + ", bootstrapServers=" + bootstrapServers + ", acks=" + acks
				+ ", retries=" + retries + ", batchSize=" + batchSize + ", lingerMs=" + lingerMs + ", bufferMemory="
				+ bufferMemory + ", keyDeserializer=" + keyDeserializer + ", valueDeserializer=" + valueDeserializer
				+ "]";
	}

}
