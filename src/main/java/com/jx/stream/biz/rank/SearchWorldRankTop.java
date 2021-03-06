
package com.jx.stream.biz.rank;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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

import com.alibaba.fastjson.JSONObject;
import com.jx.stream.utils.exam.PriorityQueueSerde;

/**
 * 提取json字段，统计数量，并发送到topic
 * 
 * @author ruilinag
 *
 */
public class SearchWorldRankTop {
	static Logger logger = LoggerFactory.getLogger(SearchWorldRankTop.class);

	static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "js-realtime";
	static final String RANK_OUP_TOPIC = "streams-wordcount-output2";

	public static void main(final String[] args) throws Exception {
		final String bootstrapServers = args.length > 0 ? args[0] : "39.108.114.201:9092";
		final Properties streamsConfiguration = new Properties();

		// application.id
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "searchWorld-rank-id");
		// client.id
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "searchWorld-rank-client");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Specify default (de)serializers for record keys and for record values.
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// 记录应该每10秒刷新一次。这比默认值要小
		// 为了保持这个示例的交互性。
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// 为了便于说明，我们禁用了记录缓存
//		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		// 数据目录 /tmp/kafka-streams D:\tmp\kafka-streams
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "D:\\tmp\\kafka-streams");
		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> textLines = builder.stream(TOP_NEWS_PER_INDUSTRY_TOPIC);

		// 格式值
		final KStream<String, String> fastValue = textLines
				// 是否是搜索词json
				.filter((dummy, record) -> iSsearchProduct(record)).map((dummy, article) -> {
					JSONObject src_value_root = (JSONObject) JSONObject.parse(article);
					JSONObject src_value = src_value_root.getJSONObject("data");
					JSONObject json_value = new JSONObject();
					// 提取需要的字段
					json_value.put("project", src_value.get("project"));// 项目
					json_value.put("search_word", src_value.getJSONObject("properties").get("search_word"));// 搜索词
					logger.info("apply return text={}", json_value.toJSONString());
					return new KeyValue<>(json_value.toJSONString(), json_value.toJSONString());
				});

		final Serde<String> k_stringSerde = Serdes.String();
		final Serde<String> stringSerde = Serdes.String();
		// Set up serializers and deserializers, which we will use for overriding the
		// default serdes
		// specified above.
		// final Serde<String> stringSerde = Serdes.String();
		final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

		final Serde<Long> longSerde = Serdes.Long();

		// 分组，
		final KTable<Windowed<String>, Long> timeWindowsGroup = fastValue
				// 计算每小时的点击量，使用一个小时的滚动窗口
				// 时间窗口，每5分钟输出一次，按key分组
				.groupByKey(Serialized.with(k_stringSerde, stringSerde))
				.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(30))).count();
		// .groupBy((key, word) -> word).count();

		// 比较器
		final Comparator<String> comparator = (o1, o2) -> (int) (JSONObject.parseObject(o2).getInteger("count")
				- JSONObject.parseObject(o1).getInteger("count"));

		final KTable<Windowed<String>, PriorityQueue<String>> allViewCounts = timeWindowsGroup.groupBy(
				// the selector
				(windowedArticle, count) -> {
					JSONObject src_value_root = (JSONObject) JSONObject.parse(windowedArticle.key());
					// project on the industry field for key
					Windowed<String> windowedIndustry = new Windowed<>(windowedArticle.key(), windowedArticle.window());
					// add the page into the value
					// 加入数量
					src_value_root.put("count", count);
					return new KeyValue<>(windowedIndustry, src_value_root.toJSONString());
				}, Serialized.with(windowedStringSerde, stringSerde)).aggregate(
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
						}, Materialized.with(windowedStringSerde,
								new PriorityQueueSerde<String>(comparator, stringSerde)));

		// 最后的值格式
		// mapValues 取一个记录，产生0、1或更多的记录。您可以修改记录键和值，包括它们的类型。
		final KTable<Windowed<String>, String> all_topViewCounts = allViewCounts.mapValues(queue -> {
			// final StringBuilder sb = new StringBuilder();
			// for (int i = 0; i < topN; i++) { 不需要排序
			final String record = queue.poll();
			if (record == null) {
				return record;
			}
			// sb.append(record.get("page").toString());
			// sb.append("\n");
			// }
			System.out.println("all_topViewCounts record.toString()=" + record.toString());
			return record.toString();
		}).filter((dummy, record) -> {
			if (dummy == null || record == null) {
				return false;
			}
			return true;
		});

		// 发送到下游
		all_topViewCounts.toStream().to(RANK_OUP_TOPIC, Produced.with(windowedStringSerde, stringSerde));

		// 开始构建
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();
		// 添加关闭钩子来响应SIGTERM并优雅地关闭
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public static boolean iSsearchProduct(String text) {
		System.out.println("text=" + text);
		logger.info("iSsearchProduct text={}", text);
		if (org.apache.commons.lang.StringUtils.isBlank(text) || !text.startsWith("{") || !text.endsWith("}")) {
			logger.info("不是json格式数据或为null={}", text);
			return false;
		}
		JSONObject src_value = (JSONObject) JSONObject.parse(text);
		String event = src_value.getJSONObject("data").getString("event");
		// 搜索词数据
		if ("searchProduct".equals(event)) {
			System.out.println("return true envent=" + event);
			return true;
		}
		return false;
	}

}
