package kafka_storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import kafka_storm.bolt.WordSpliter;
import kafka_storm.bolt.WriterBolt;
import kafka_storm.spout.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaTopo {
	public static void main(String[] args) throws Exception {
		String topic = "wordcount";
		String zkRoot = "/kafka-storm";
		String spoutId = "KafakaSpout";
		BrokerHosts brokerHosts = new ZkHosts("it01:2181,it02:2181,it03:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		spoutConfig.forceFromStart = true;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		// 设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
		builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
		builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping(spoutId);
		builder.setBolt("writer", new WriterBolt("/Users/xmy/Desktop/wc/","WC")).fieldsGrouping("word-spilter", new Fields("word"));

		Config config = new Config();
		// 设置worker并发数量
		config.setNumWorkers(4);
		// 事务并发数量
//		config.setNumAckers(4); 
		// dubug 支持
		config.setDebug(false);
		// LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", config, builder.createTopology());
		// 提交topology到storm集群中运行
//		StormSubmitter.submitTopology("WordCount", config, builder.createTopology());

	}
}
