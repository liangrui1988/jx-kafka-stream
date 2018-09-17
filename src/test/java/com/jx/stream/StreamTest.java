package com.jx.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class StreamTest {

	public StreamTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsConfig config = new StreamsConfig(props);
		KStreamBuilder builder = new KStreamBuilder();
		builder.stream("my-input-topic").mapValues(value -> value.toString()).to("my-output-topic");
		;
		// builder.stream("my-input-topic").mapValues(value ->
		// value.length().toString()).to("my-output-topic");
		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();

	}

	public void test1() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsConfig config = new StreamsConfig(props);

		final Serializer<String> stringserializer = new StringSerializer();
		final Deserializer<String> deserializer = new StringDeserializer();
		final Serializer<Long> longserializer = new LongSerializer();
		final Deserializer<Long> longdeserializer = new LongDeserializer();

		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> textLine = builder.stream("textLinesTopic");

		// KStream<String, String> textLine = builder.stream(deserializer,
		// deserializer, s );
		// Split each text line, by whitespace, into words.

		// Ensure the words are available as record keys for the next
		// aggregate operation.

		// Count the occurrences of each word (record key) and store the
		// results into a table named "Counts".
	textLine.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("W+"))).map((key, value) -> new KeyValue<>(value, value));
		
				
//		KTable wordCounts = textLine.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("W+"))).map((key, value) -> new KeyValue<>(value, value)).countByKey("Counts");

		builder.stream("my-input-topic").mapValues(value -> value.toString()).to("my-output-topic");

		// builder.stream("my-input-topic").mapValues(value ->
		// value.length().toString()).to("my-output-topic");
		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();

	}

}
