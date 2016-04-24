package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.BoltCreatBF;
import storm.bolt.BoltTest;
import storm.bolt.NameBolt;
import storm.spout.RDFSpout;
import storm.spout.SpoutTest;

public class TestTopology {
	public static BufferedReader reader;
	public static void main(String[] args) throws Exception{
		

		String filepath = "/Users/tjstone/Documents/workspace/ContinuousRDF/data.txt";
		File file = new File(filepath);
		
		reader = null;
		try{
			reader = new BufferedReader(new FileReader(file));
			stormCall();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();	
					
		} catch (IOException e){
			//do nothing
		}finally{
			if(reader != null){
				try{
					reader.close();
				}catch(IOException e1){
					//Do nothing
				}
			}
		}
	}
	public void setReader(BufferedReader newReader){
		this.reader = newReader;
	}
	
	
	public static void stormCall() throws InterruptedException
	{
		Config config = new Config();
		config.setDebug(true);

		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * Bolts to create bloom filters using fieldsGrouping on Predicate. 
		 * For now we are creating 3 bloomfilters for each predicate.
		*/
		builder.setSpout("spout_getdata", new SpoutTest(),1);
		//write own bolt, insert here, 3 is the parallelism factor of the bolts
		//now creates a bloomfilter for every triple
		builder.setBolt("getname", new NameBolt(),1).fieldsGrouping("spout_getdata", new Fields("Name"));
		//BoltCreateTest() handles new data after the Bloomfilters have been created


		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TestStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		//Sander: cluster shutdown throws IOException, but adding try/catch states that it is an Unreachable catch block for IOException.
		try{
			cluster.shutdown();	
			throw new IOException("test");//Used as debug, otherwise we got the error saying this block couldn't generate an IOException
		} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}
	
		
		/* Result like this
		Bloom Filter with Predicate = Work has values = 11
		Bloom Filter with Predicate = Paper has values = 5
		Bloom Filter with Predicate = Diplome has values = 13
		*/
	}
	
	
	
	
}