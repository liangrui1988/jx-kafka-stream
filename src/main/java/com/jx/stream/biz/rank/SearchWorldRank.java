
package com.jx.stream.biz.rank;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * 提取json字段，统计数量，并发送到topic
 * 
 * @author ruilinag
 *
 */
public class SearchWorldRank {
	static Logger logger = LoggerFactory.getLogger(SearchWorldRank.class);

	static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "streams-file-input";
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
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		// In the subsequent lines we define the processing topology of the Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();

		final KStream<String, String> textLines = builder.stream(TOP_NEWS_PER_INDUSTRY_TOPIC);

		final KStream<String, String> wordViews = textLines
				// 是否是搜索词json
				.filter((dummy, record) -> iSsearchProduct(record))
				// 输入一条记录，输出一条经过变换的记录。
				.mapValues(new ValueMapper<String, String>() {
					@Override
					public String apply(String value) {
						JSONObject src_value = (JSONObject) JSONObject.parse(value);
						JSONObject json_value = new JSONObject();
						// 提取需要的字段
						json_value.put("project", src_value.get("project"));// 项目
						json_value.put("search_word", src_value.getJSONObject("properties").get("search_word"));// 搜索词
						return json_value.toJSONString();
					}
				});

		final Serde<String> k_stringSerde = Serdes.String();
		final Serde<String> v_stringSerde = Serdes.String();
		// final Serde<String> v_stringSerde = Serdes.String();
		final KTable<Windowed<String>, Long> wordCounts = wordViews
				// 计算每小时的点击量，使用一个小时的滚动窗口
				// 时间窗口，每5分钟输出一次，按key分组
				.groupByKey(Serialized.with(k_stringSerde, v_stringSerde))
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5))).count();
		// .groupBy((key, word) -> word).count();

		wordCounts.toStream().to(RANK_OUP_TOPIC);

		// 开始构建
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();
		// 添加关闭钩子来响应SIGTERM并优雅地关闭
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public static boolean iSsearchProduct(String text) {
		logger.debug("iSsearchProduct text={}", text);
		if (org.apache.commons.lang.StringUtils.isBlank(text) || !text.startsWith("{") || text.endsWith("}")) {
			logger.info("不是json格式数据或为null={}", text);
			return false;
		}
		JSONObject src_value = (JSONObject) JSONObject.parse(text);
		String envent = src_value.getString("envent");
		// 搜索词数据
		if ("searchProduct".equals(envent)) {
			return true;
		}
		return false;
	}

}
