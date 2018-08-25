package storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UpperBolt extends BaseBasicBolt{

	private static final long serialVersionUID = -7083567290263234755L;
	//业务逻辑
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//先获取上一组件传递过来的数据,数据在tuple里面
		String goodName = tuple.getString(0);
		
		//将商品名转换成大写
		String goodName_upper = goodName.toUpperCase();
		//将转换完成的商品名发送出去
//		List<Object> tup = new ArrayList<Object>();
//		tup.add(goodName_upper);
		collector.emit(new Values(goodName_upper));
		
	}
	//声明该bolt组件要发送出去的tuple的字段
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uppername"));
	}

}
