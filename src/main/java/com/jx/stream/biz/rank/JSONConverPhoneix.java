
package com.jx.stream.biz.rank;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jx.stream.utils.exam.PriorityQueueSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

/**
 * json格式简单转换phoneix表CommonRank对应的字段，去除不用的字段。
 * 
 * @date 2018-09-26
 * 
 * @author ruiliang
 *
 */
public class JSONConverPhoneix {
	static Logger logger = LoggerFactory.getLogger(JSONConverPhoneix.class);

	// 输出的 topic
	static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "sinkPhoneixCommonRank";
	// 读取的topic
	static final String PAGE_VIEWS = "js-realtime";

	private static boolean isArticle(final GenericRecord record) {
		// 一个Utf8字符串
		final Utf8 flags = (Utf8) record.get("flags");
		if (flags == null) {
			return false;
		}
		logger.info("filter flags text={}", flags);
		return flags.toString().contains("ARTICLE");
	}

	public static void main(final String[] args) throws Exception {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:9091";
		final KafkaStreams streams = buildStream(bootstrapServers, "/tmp/kafka-streams");
		streams.cleanUp();
		streams.start();
		// 添加关闭钩子来响应SIGTERM并优雅地关闭Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	static KafkaStreams buildStream(final String bootstrapServers, final String stateDir) throws IOException {
		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique in the
		// Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "top-articles-lambda-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Where to find the Confluent schema registry instance(s)
		// 在哪里可以找到 Confluent schema 模式注册表实例（s）
		// streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
		// schemaRegistryUrl);
		// 为记录键和记录值指定默认（反）序列化器。
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		// 记录应该每10秒刷新一次。这比默认值要小
		// 为了保持这个示例的交互性。
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

		// Serdes used in this
		final Serde<String> stringSerde = Serdes.String();

		final Map<String, String> serdeConfig = Collections
				.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");

		// GenericRecord 一个记录模式的通用实例。字段可以通过名称访问 就像指数一样
		final Serde<GenericRecord> keyAvroSerde = new GenericAvroSerde();
		keyAvroSerde.configure(serdeConfig, true);

		final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
		valueAvroSerde.configure(serdeConfig, false);

		final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

		final StreamsBuilder builder = new StreamsBuilder();

		// 取出topic数据流
		final KStream<byte[], GenericRecord> views = builder.stream(PAGE_VIEWS);

		// 解析avsc文件的Schema
		final InputStream statsSchema = JSONConverPhoneix.class.getClassLoader()
				.getResourceAsStream("avro/io/confluent/examples/streams/pageviewstats.avsc");
		final Schema schema = new Schema.Parser().parse(statsSchema);

		final KStream<GenericRecord, GenericRecord> articleViews = views
				// filter only article pages
				.filter((dummy, record) -> isArticle(record))
				// .map 将输入流的每条记录转换为输出流中的新纪录（键和值类型都可以是任意改变)
				// map <page id, industry> as key by making user the same for each record
				// 通过使每个记录的用户相同，将<页面id，工业作为关键
				.map((dummy, article) -> {
					final GenericRecord clone = new GenericData.Record(article.getSchema());
					clone.put("user", "user");
					clone.put("page", article.get("page"));
					clone.put("industry", article.get("industry"));
					return new KeyValue<>(clone, clone);
				});

		final KTable<Windowed<GenericRecord>, Long> viewCounts = articleViews
				// count the clicks per hour, using tumbling windows with a size of one hour
				.groupByKey(Serialized.with(keyAvroSerde, valueAvroSerde))
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(60))).count();

		final Comparator<GenericRecord> comparator = (o1,
				o2) -> (int) ((Long) o2.get("count") - (Long) o1.get("count"));

		final KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts.groupBy(
				// the selector
				(windowedArticle, count) -> {
					// project on the industry field for key
					Windowed<String> windowedIndustry = new Windowed<>(windowedArticle.key().get("industry").toString(),
							windowedArticle.window());
					// add the page into the value
					GenericRecord viewStats = new GenericData.Record(schema);
					viewStats.put("page", windowedArticle.key().get("page"));
					viewStats.put("user", "user");
					viewStats.put("industry", windowedArticle.key().get("industry"));
					viewStats.put("count", count);
					return new KeyValue<>(windowedIndustry, viewStats);
				}, Serialized.with(windowedStringSerde, valueAvroSerde)).aggregate(
						// 合计原始记录的价值,和老数据聚合
						// the initializer
						() -> new PriorityQueue<>(comparator),

						// the "add" aggregator
						(windowedIndustry, record, queue) -> {
							queue.add(record);
							return queue;
						},

						// the "remove" aggregator
						(windowedIndustry, record, queue) -> {
							queue.remove(record);
							return queue;
						},

						Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, valueAvroSerde)));

		final int topN = 100;
		// mapValues 取一个记录，产生0、1或更多的记录。您可以修改记录键和值，包括它们的类型。
		final KTable<Windowed<String>, String> topViewCounts = allViewCounts.mapValues(queue -> {
			final StringBuilder sb = new StringBuilder();
			for (int i = 0; i < topN; i++) {
				final GenericRecord record = queue.poll();
				if (record == null) {
					break;
				}
				sb.append(record.get("page").toString());
				sb.append("\n");
			}
			return sb.toString();
		});

		topViewCounts.toStream().to(TOP_NEWS_PER_INDUSTRY_TOPIC, Produced.with(windowedStringSerde, stringSerde));
		return new KafkaStreams(builder.build(), streamsConfiguration);
	}

}
