package storm;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SuffixBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1419749730861962841L;
	FileWriter fileWriter = null;
	public static String path="/usr/hadoop/apache-storm-0.9.2/stormoutput/";
	//类加载前 加载一次prepare方法 创建一个fileWriter
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fileWriter = new FileWriter(path+UUID.randomUUID());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	//该bolt组件的核心处理逻辑
	//每手袋一个tuple消息,就会被调用一次
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//先拿到上一组件发送过来的商品名称
		String upper_name = tuple.getString(0);
		String suffix_name = upper_name + "_itisok";
		
		try {
			fileWriter.write(suffix_name);
			fileWriter.write("\n");
			fileWriter.flush();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
