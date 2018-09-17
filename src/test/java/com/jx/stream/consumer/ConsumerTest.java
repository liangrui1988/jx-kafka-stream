package com.jx.stream.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.jx.stream.builder.ConsumerBuilder;


public class ConsumerTest {

	public ConsumerTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		KafkaConsumer<String, String> c=new ConsumerBuilder().groupId("t1").kafkaConsumerBuilder();
		
		System.out.println(c);
	}

	@Test
	public void test1() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");// 自动偏移增长
		props.put("auto.commit.interval.ms", "1000");// 设置`enable.auto.commit`,偏移量由`auto.commit.interval.ms`控制自动提交的频率。

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo", "bar"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
																			// poll”进行阻塞，直到数据到达（并且设置等待时间的好处是可以积累消息，组成大数据块一并发送）。
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}

	@Test
	public void utilsTest() {
		KafkaConsumer<String, String> consumer = new ConsumerBuilder().groupId("test").kafkaConsumerBuilder();
		consumer.subscribe(Arrays.asList("foo", "bar"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
																			// poll”进行阻塞，直到数据到达（并且设置等待时间的好处是可以积累消息，组成大数据块一并发送）。
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}
	
	@Test
	public void testTmp() {
		KafkaConsumer<String, String> consumer = new ConsumerBuilder().valueDeserializer("org.apache.kafka.common.serialization.StringDeserializer").groupId("test1").kafkaConsumerBuilder();
		consumer.subscribe(Arrays.asList("streams-wordcount-Counts-repartition"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
																			// poll”进行阻塞，直到数据到达（并且设置等待时间的好处是可以积累消息，组成大数据块一并发送）。
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}
	@Test
	public void testTmp2() {
		KafkaConsumer<String, String> consumer = new ConsumerBuilder().valueDeserializer("org.apache.kafka.common.serialization.LongDeserializer").groupId("test1").kafkaConsumerBuilder();
		consumer.subscribe(Arrays.asList("md-streams-wordcount-output"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
																			// poll”进行阻塞，直到数据到达（并且设置等待时间的好处是可以积累消息，组成大数据块一并发送）。
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + "wordcount:offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}
	

	/**
	 * 用户不依赖于使用者定期提交消费补偿，也可以在记录应该被视为消费时控制，从而提交他们的偏移量。当消息的消费与某些处理逻辑耦合在一起时，这是有用的，
	 * 因此在完成处理之前，不应该将消息视为已被使用。
	 */
	@Test
	public void ManualOffset() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo", "bar"));
		final int minBatchSize = 200;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// 100milliseconds时间
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
			if (buffer.size() >= minBatchSize) {
				// insertIntoDb(buffer);
				System.out.println("buffer:" + buffer);
				consumer.commitSync();// 阻塞偏移位置 ,处理数据后提交
				// commitAsync是非阻塞式的，当成功或失败时，会引发OffsetCommitCallback。
				buffer.clear();
			}
		}

	}

	/**
	 * 
	 * 
	 * 
	 * 上面的例子使用commitSync表示所有收到的消息为”已提交"，在某些情况下，你可以希望更精细的控制，通过指定一个明确消息的偏移量为“
	 * 已提交”。在下面，我们的例子中，我们处理完每个分区中的消息后，提交偏移量。
	 */
	public void t2() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.20.243:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo", "bar"));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.println(record.offset() + ": " + record.value());
					}
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}
			}
		} finally {
			consumer.close();
		}
	}

}
