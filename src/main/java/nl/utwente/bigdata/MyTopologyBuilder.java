package nl.utwente.bigdata;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class MyTopologyBuilder extends TopologyBuilder {
	public enum Connection {
		SHUFFLE,
		FIELDS,
		GLOBAL,
	}

	/**
	 * @param abstractTopologyRunner
	 */
	MyTopologyBuilder() {
		super();
	}

	private String prevId = null;
	
	public SpoutDeclarer addSpout(String id, IRichSpout spout) {
		SpoutDeclarer sd = setSpout(id, spout);
		prevId = id;
		return sd;
	}
	
	public void addShuffledBolt(String id, IBasicBolt bolt) {
		addBolt(id, bolt, Connection.SHUFFLE);
	}
	
	public void addShuffledBolt(String id, IRichBolt bolt) {
		addBolt(id, bolt, Connection.SHUFFLE);
	}
	
	public void addGlobalBolt(String id,  IBasicBolt bolt) {
		addBolt(id, bolt, Connection.GLOBAL);
	}
	
	public void addBolt(String id,  IRichBolt bolt, Connection type) {
		BoltDeclarer bd = super.setBolt(id, bolt);
		if (prevId != null) {
			switch (type) {
			case SHUFFLE:
				bd.shuffleGrouping(prevId);
				break;
			case FIELDS:
				//bd.fieldsGrouping(componentId, fields);
				break;
			case GLOBAL:
				bd.globalGrouping(prevId);
				break;
			}
			
		}
		System.out.println(id + " " + prevId);
		prevId = id;
	}
	
	public void addBolt(String id,  IBasicBolt bolt, Connection type) {
		BoltDeclarer bd = super.setBolt(id, bolt);
		if (prevId != null) {
			switch (type) {
			case SHUFFLE:
				bd.shuffleGrouping(prevId);
				break;
			case FIELDS:
				//bd.fieldsGrouping(componentId, fields);
				break;
			case GLOBAL:
				bd.globalGrouping(prevId);
				break;
			}
			
		}
		System.out.println(id + " " + prevId);
		prevId = id;
	}
	
	public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
		prevId = id;
		return super.setBolt(id, bolt);
	}
}