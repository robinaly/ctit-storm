package nl.utwente.bigdata;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

public abstract class AbstractTopologyRunner {
	
	// sub-classes have to override this function
	protected abstract void buildTopology(MyTopologyBuilder builder, Properties properties);
	
	public void fillConf(Config conf) {
		
	}
	
	public void runLocal(String name, Properties properties) { 
    	MyTopologyBuilder builder = new MyTopologyBuilder();
    	buildTopology(builder, properties);
    	StormTopology topology = builder.createTopology();
        Config conf = new Config();
        for (Object key: properties.keySet()) {
        	conf.put(String.valueOf(key), properties.get(key));
        }
        fillConf(conf);
        LocalCluster cluster = new LocalCluster();        
        cluster.submitTopology(name, conf, topology);        
        Utils.sleep(Integer.parseInt(properties.getProperty("sleep", 10 * 60 * 1000 + "")));
        cluster.shutdown();
    }
    
    // start 
    public void runCluster(String name, Properties properties) throws AlreadyAliveException, InvalidTopologyException {
    	MyTopologyBuilder builder = new MyTopologyBuilder();
    	buildTopology(builder, properties);
    	StormTopology topology = builder.createTopology();    	
        Config conf = new Config(); 
        for (Object key: properties.keySet()) {
        	conf.put(String.valueOf(key), properties.get(key));
        }
        conf.put(Config.NIMBUS_HOST, properties.getProperty("nimbus", "localhost"));
        fillConf(conf);
        StormSubmitter.submitTopology(name, conf, topology);
    }
    
    // Starts a topology based on it's command line 
    public void run(String[] args) {
    	try {
        	String name = args[0];
        	String type = args[1];
        	Properties properties = new Properties();        	
        	if (args.length > 2) {
        		properties.load(new FileInputStream(args[2]));
        	}
        	if (type.equals("local")) {
        		runLocal(name, properties);
        	} else {
        		runCluster(name, properties);
        	}

		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
}
