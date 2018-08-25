package storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomWordSpout extends BaseRichSpout {
	private static final long serialVersionUID = -1135637204076073900L;
	private SpoutOutputCollector collector;
	// 模拟数据
	String[] words = { "iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu" };

	public void nextTuple() {
		// 可以从kafka消息队列中拿数据,简便起见,从words数组随机挑选
		Random random = new Random();
		int index = random.nextInt(words.length);
		// 通过随机数拿到的一个商品名
		String goodName = words[index];

		// 将商品封装成tuple,发送消息给下一个组件
		collector.emit(new Values(goodName));

		Utils.sleep(500);

	}

	// 初始化方法 ,在spout实例化时候调用一次
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	// 声明本spout组件发送出去的tuple中的数据的字段名
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orignname"));
	}

}
