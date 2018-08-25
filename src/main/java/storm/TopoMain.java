package storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class TopoMain {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// spout组件设置到topology中去
		builder.setSpout("randomspout", new RandomWordSpout(), 4);

		// 将大写转换bolt组件设置到topology,并指定它接受randomspout组件的消息
		builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");

		// 将添加后缀的bolt组件设置到topology,并指定它接受upperbolt组件的消息
		builder.setBolt("suffixbolt", new SuffixBolt(), 4).shuffleGrouping("upperbolt");

		// 用builder来创建一个topology
		StormTopology demotopo = builder.createTopology();

		// 配置一下topology在集群中运行时的参数
		Config conf = new Config();
		// Worker数
		conf.setNumWorkers(4);
		//日志节点
		conf.setDebug(true);
		//事务设置
		conf.setNumAckers(0);

		// 将这个topology提交给storm集群运行
		StormSubmitter.submitTopology("demotopo", conf, demotopo);
	}
}
