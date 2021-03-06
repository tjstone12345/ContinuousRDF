package storm.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltProberWithThreeBF implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter<String> bfp1;
	private BloomFilter<String> bfp2;
	
	private BloomFilter<String> bfp3;
	public static List<Tuple> queryResult;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.bfp1 = new BloomFilter(0.01, 10);
		this.bfp2 = new BloomFilter(0.01, 10);

		this.bfp3 = new BloomFilter(0.01, 10);
		queryResult = new ArrayList<Tuple>();
		
	}

	public void execute(Tuple tuple) {
		
		String jointype = tuple.getStringByField("JoinType");
		
		if (jointype.equalsIgnoreCase("onevariable")) {
			oneVariableJoin(tuple);
		} else if (jointype.equalsIgnoreCase("twovariable")){
			twoVariableJoin(tuple);
		} else if (jointype.equalsIgnoreCase("multivariable")){
			multiVariableJoin(tuple);
		}
		else {
			System.out.println("Error, con't identify join type");
		}
	}

	
	public void oneVariableJoin(Tuple tuple) {
		
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if(id[0].equals("BuilderTaskID")){
			if(id[1].equals("1")){
				bfp1.add(tuple.getStringByField("Content"));
				
			}else{
				bfp2.add(tuple.getStringByField("Content"));
			}
		}else{
			boolean contains1 = bfp1.contains(tuple.getStringByField("Content"));
			boolean contains2 = bfp2.contains(tuple.getStringByField("Content"));
			if(contains1 && contains2){
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple);
			}
		}
	}
	
	public void twoVariableJoin(Tuple tuple) {
		
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if(id[0].equals("BuilderTaskID")){
			if(id[1].equals("1")){
				bfp1.add(tuple.getStringByField("Content"));
			}else{
				bfp2.add(tuple.getStringByField("Content"));
			}
		}else{
			boolean contains1 = bfp1.contains(tuple.getStringByField("Content"));
			boolean contains2 = bfp2.contains(tuple.getStringByField("Content"));
			if(contains1 && contains2){
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple);
			}
		}
	}
	
	public void multiVariableJoin(Tuple tuple) {
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if(id[0].equals("BuilderTaskID")) {
			if(id[1].equals("1")) {
				boolean contains2 = bfp2.contains(tuple.getStringByField("Content"));
				boolean contains3 = bfp3.contains(tuple.getStringByField("Content"));
				if(contains2 && contains3){
					collector.emit(new Values(tuple.getStringByField("Content")));
					queryResult.add(tuple);
				} else { 
					bfp1.add(tuple.getStringByField("Content"));
				}
				
			} else {
				boolean contains1 = bfp1.contains(tuple.getStringByField("Content"));
				boolean contains3 = bfp3.contains(tuple.getStringByField("Content"));
				if(contains1 && contains3) {
					collector.emit(new Values(tuple.getStringByField("Content")));
					queryResult.add(tuple);
				} else {
					bfp2.add(tuple.getStringByField("Content"));
				}
			}
		} else {
			
			boolean contains1 = bfp1.contains(tuple.getStringByField("Content"));
			boolean contains2 = bfp2.contains(tuple.getStringByField("Content"));
			if(contains1 && contains2) {
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple);
			} 
			else {
				bfp3.add(tuple.getStringByField("Content"));
			}
			
		}
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public void cleanup() {
		System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
