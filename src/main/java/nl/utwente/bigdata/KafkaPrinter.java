package nl.utwente.bigdata;

import java.util.Map;
import java.util.Properties;

import nl.utwente.bigdata.bolts.TimelineBolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Maps;

public class KafkaPrinter extends AbstractTopologyRunner {

	@Override
	public void fillConf(Config conf) {
		Map<String, String> HBConfig = Maps.newHashMap();
	    HBConfig.put("hbase.rootdir","hdfs://ctit048.ewi.utwente.nl/hbase");
	    HBConfig.put("hbase.zookeeper.quorum", "ctit048.ewi.utwente.nl:2181");
	    conf.put("HBCONFIG",HBConfig);
	    
	}
	
	@Override
	protected void buildTopology(MyTopologyBuilder builder, Properties properties) {
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts(properties.getProperty("zkhost", "ctit048.ewi.utwente.nl:2181")),
				  properties.getProperty("topic", "test"), // topic to read from
				  "", // the root path in Zookeeper for the spout to store the consumer offsets
				  properties.getProperty("consumer", "test"));
		//spoutConf.forceFromStart = true;
		spoutConf.scheme = new TextFormat();
		KafkaSpout spout = new KafkaSpout(spoutConf);
		
		builder.addSpout("kafka", spout);
		
		builder.addShuffledBolt("timeline", new TimelineBolt());
		
		SimpleHBaseMapper mapper = new SimpleHBaseMapper() 
        .withRowKeyField(properties.getProperty("key", "id"))
        .withColumnFields(new Fields(properties.getProperty("key", "tweet")))
        //.withCounterFields(new Fields("count"))
        .withColumnFamily(properties.getProperty("columnfamily", "cf"));
		HBaseBolt hbase = new HBaseBolt("test", mapper).withConfigKey("HBCONFIG");
		
		builder.addShuffledBolt("hbase", hbase);
		
		//
		//builder.addBolt("printer", new PrinterBolt(), MyTopologyBuilder.Connection.SHUFFLE);
	}

}
