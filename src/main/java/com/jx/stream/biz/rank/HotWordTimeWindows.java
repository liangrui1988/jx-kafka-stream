package com.jx.stream.biz.rank;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间窗口
 * <p>
 * Hopping Time Window它有两个属性，一个是Window size，一个是Advance interval。Window
 * size指定了窗口的大小，也即每次计算的数据集的大小。而Advance interval定义输出的时间间隔。
 * <p>
 * Tumbling Time Window 可以认为它是Hopping Time Window的一种特例，也即Window size和Advance
 * interval相等。它的特点是各个Window之间完全不相交。
 * <p>
 * Sliding Window该窗口只用于2个KStream进行Join计算时。
 * <p>
 * Session Window该窗口用于对Key做Group后的聚合操作中。
 * 
 * @author ruiliang
 * @date 2018-09-19
 *
 */
public class HotWordTimeWindows {
	static Logger logger = LoggerFactory.getLogger(HotWordTimeWindows.class);

	static final String STREAMS_WINDOWS_INTPUT = "streams-file-input-rank";// 消费的topic
	static long windowSizeMs = TimeUnit.MINUTES.toMillis(5); // 5 * 60 * 1000L
	static long advanceMs = TimeUnit.MINUTES.toMillis(1); // 1 * 60 * 1000L
	static final String STREAMS_WINDOWS_OUTPUT = "streams-wordcount-output-rank";// 流出的topic

	public static void main(String[] args) {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
		final KafkaStreams streams = createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams");
		streams.cleanUp();
		streams.start();
		logger.info("start HotWordTimeWindows main");
		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	static KafkaStreams createStreams(final String bootstrapServers, final String schemaRegistryUrl,
			final String stateDir) {
		final Properties config = new Properties();
		// Give the Streams application a unique name. The name must be unique in the
		// Kafka cluster
		// against which the application is run.
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rank-windows-example");
		config.put(StreamsConfig.CLIENT_ID_CONFIG, "rank-windows-example-client");
		// Where to find Kafka broker(s).
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		// Set to earliest so we don't miss any data that arrived in the topics before
		// the process
		// started
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// disable caching to see session merging
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final StreamsBuilder builder = new StreamsBuilder();
		builder.stream(STREAMS_WINDOWS_INTPUT, Consumed.with(Serdes.String(), Serdes.Long()))
				// group by key so we can count by session windows
				// .groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
				.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
				// window by
				.windowedBy(TimeWindows.of(windowSizeMs).advanceBy(advanceMs))
				// count play events per session
				.count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(STREAMS_WINDOWS_OUTPUT)
						.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
				// convert to a stream so we can map the key to a string
				.toStream()
				// map key to a readable string
				.map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(),
						value))
				// write to play-events-per-session topic
				.to(STREAMS_WINDOWS_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

		return new KafkaStreams(builder.build(), new StreamsConfig(config));
	}

}
