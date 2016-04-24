package storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NameBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String name = input.getStringByField("Name");
		System.out.println(name);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
	
	
}