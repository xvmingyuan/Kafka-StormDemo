package kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("zk.connect", "it01:2181,it02:2181,it03:2181");
		props.put("metadata.broker.list", "it01:9092,it02:9092,it03:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		for (int i = 1; i < 500; i++) {
			Thread.sleep(250);
			producer.send(new KeyedMessage<String, String>("mygirls", "I said i love you baby for" + i + "times!"));

		}
	}
}
