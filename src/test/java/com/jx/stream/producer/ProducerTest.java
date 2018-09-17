package com.jx.stream.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.jx.stream.builder.ProducerBuilder;

public class ProducerTest {

	public ProducerTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		// props.put("metadata.broker.list", "192.168.20.243:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

		}

		producer.close();
	}

	@Test
	public void testUtil() {
//		Producer<String, String> producer = KafkaClientUtils.getProducer();
//		producer.send(new ProducerRecord<String, String>("test3", "msg-key", "msg-v"));
//		producer.close();
//		Properties properties=KafkaClientUtils.getDefultProperties();
//		properties.put("bootstrap.servers", "192.168.20.243:9093");
//		Producer<String, String> producer2 = KafkaClientUtils.getProducer();
		
		Producer<String, String> producer3 =new ProducerBuilder().kafkaProducerBuilder();
		producer3.send(new ProducerRecord<String, String>("test", "msg-key", "msg-ProducerBuilder"));
		producer3.close();
	}

	@Test
	public void testAdd() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		// props.put("metadata.broker.list", "192.168.20.243:9092");
		props.put("group.id", "test");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);// 指示生产者在发送请求之前等待数毫秒的时间,可能所有的100条记录都将被发送到一个请求中
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 200; i++) {
			producer.send(new ProducerRecord<String, String>("bar", Integer.toString(i), Integer.toString(i)));
			// producer.send(new ProducerRecord<String, String>("bar",
			// Integer.toString(i), Integer.toString(i)));

		}

		producer.close();
	}

	@Test
	public void transactional() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		props.put("transactional.id", "my-transactional-id");
		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

		producer.initTransactions();

		try {
			producer.beginTransaction();
			for (int i = 0; i < 100; i++)
				producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i) + "v"));
			producer.commitTransaction();
		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			// We can't recover from these exceptions, so our only option is to
			// 我们无法从这些异常中恢复，所以我们唯一的选择是
			// close the producer and exit.
			producer.close();
		} catch (KafkaException e) {
			// For all other exceptions, just abort the transaction and try
			// 对于所有其他异常，只需中止事务并再次尝试
			producer.abortTransaction();
		}
		producer.close();
	}

}
