package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerDemo {
	private static final String topic = "mysons";
	private static final Integer threads = 1;

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "it01:2181,it02:2181,it03:2181");
		props.put("group.id", "1111");
		props.put("auto.offset.reset", "smallest");
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		// 添加话题,指定线程数
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		topicCountMap.put("mygirls", 1);
		// 获取全部话题信息<topic,消息>
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		// 获取topic=mygirls的消息
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("mygirls");
		
		for (final KafkaStream<byte[], byte[]> kafkaStream : streams) {
			new Thread(new Runnable() {
				public void run() {
					for (MessageAndMetadata<byte[], byte[]> mm : kafkaStream) {
						String msg = new String(mm.message());
						System.out.println(msg);
					}
				}
			}).start();

		}
	}
}
