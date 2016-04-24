package storm.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;



import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.topology.IVJTopology;
import storm.topology.TestTopology;

public class SpoutTest extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	BufferedReader _reader; 

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		this._rand = new Random();
		//to read the input file
		this._reader = TestTopology.reader;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(100);
		try {
			generateTuple();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void generateTuple() throws IOException{
		String tempString = null;
		while((tempString = _reader.readLine())!=null){
			String[] parts = tempString.split(" +");
			String Name = parts[0];
			String Surname = parts[1];
			_collector.emit(new Values(Name, Surname));
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
}