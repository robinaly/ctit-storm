package nl.utwente.bigdata.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MovingAvgBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	
	private int W = 5; // the number of 
	private double[] window = new double[W]; // array of readings
	private double sum = 0.0; // current sum
	private int i = 0; // position inside window
	
	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		if (stormConf != null && stormConf.containsKey("W")) {
			W = Integer.parseInt((String) stormConf.get("W"));
			window = new double[W];
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO: add here the functionality that accumulates the W values. 
		// Once there are enough values, every received tuple also causes 
		// the emission of the average over the last W values;
		// BEGIN SOLUTION
		Double val = tuple.getDouble(0);
		int pos = i % W;
		sum -= window[pos];
		sum += val;
		window[pos] = val;
		i++;
		if (i >= W) collector.emit(new Values(sum / W));
		// END SOLUTION
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("average"));
	}
}