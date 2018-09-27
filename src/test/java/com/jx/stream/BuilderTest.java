package com.jx.stream;

import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import com.jx.stream.builder.ConsumerBuilder;
import com.jx.stream.builder.ProducerBuilder;

public class BuilderTest {

	// @Test
	// public void producerUtil() {
	// System.out.println("发送kafka........");
	// Producer<String, String> producer3 = new
	// ProducerBuilder().kafkaProducerBuilder();
	// producer3.send(new ProducerRecord<String, String>("foo", "k-1", "msg-foo-v" +
	// new Date()));
	// producer3.send(new ProducerRecord<String, String>("bar", "k-2", "msg-bar-v" +
	// new Date()));
	// producer3.close();
	// }

	@Test
	public void streamTest() {
		System.out.println("发送kafka........");
		Producer<String, String> producer3 = new ProducerBuilder().kafkaProducerBuilder();
		for (int i = 0; i < 10; i++) {
			String json = "{\"data\":{\"user_id\":\"143\",\"distinct_id\":\"web-9b55e344ae594562bdefd5de21fa855b\",\"project\":\"joyxuan_mall\",\"time\":1538020148136,\"event\":\"searchProduct\",\"type\":\"distribution\",\"real_user_id\":\"3ed96ee9-c1af-4891-aff2-f8f79beb8b81\",\"properties\":{\"referer\":\"\",\"screen_width\":2133,\"screen_height\":993,\"product_type_one\":\"\",\"app_version\":\"\",\"product_type_two\":\"\",\"is_login_id\":true,\"channel\":\"H5\",\"search_word\":\"hello\",\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36\"},\"url\":\"http://test.wx.joyxuan.com/wx_html/index.html#/SearchResult?keywords=hello\"},\"ip\":\"119.129.71.32\"}";
			producer3.send(new ProducerRecord<String, String>("js-realtime", json));
		}
		producer3.close();
	}

	@Test
	public void consumerTest() {
		System.out.println("接收kafka........");
		KafkaConsumer<String, String> consumer = new ConsumerBuilder().groupId("test").kafkaConsumerBuilder();
		consumer.subscribe(Arrays.asList("streams-wordcount-output2"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);// “long
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf(i + ":offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
						record.value());

			}
		}
	}

	// @Test
	// public void consumerTest2() {
	// System.out.println("接收kafka Group2........");
	// KafkaConsumer<String, String> consumer = new
	// ConsumerBuilder().groupId("test3").kafkaConsumerBuilder();
	// consumer.subscribe(Arrays.asList("foo", "bar"));
	// while (true) {
	// ConsumerRecords<String, String> records = consumer.poll(100);// “long
	// int i = 0;
	// for (ConsumerRecord<String, String> record : records) {
	// i++;
	// System.out.printf(i + ":offset = %d, key = %s, value = %s%n",
	// record.offset(), record.key(),
	// record.value());
	//
	// }
	// }
	// }
}
