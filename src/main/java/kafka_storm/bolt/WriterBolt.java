package kafka_storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7356882040629325405L;
	private FileWriter fileWriter = null;
	private String path = "/Users/xmy/Desktop/wc/";
	private String keyword = "wordcount";

	public WriterBolt() {
	}

	public WriterBolt(String path, String keyword) {
		this.path = path;
		this.keyword = keyword;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fileWriter = new FileWriter(path + keyword + UUID.randomUUID().toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String string = tuple.getString(0);
		try {
			fileWriter.write(string);
			fileWriter.write("\n");
			fileWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
