package nl.utwente.bigdata.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NGramerBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4031901444200770796L;
	private long N = 2;

	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		if (stormConf != null && stormConf.containsKey("N")) {
			N = Long.parseLong((String) stormConf.get("N"));
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String text = tuple.getString(0);
		text = text.replaceAll("[^A-Za-z0-9\\@\\#\\s,]","");
		text = text.toLowerCase();
		String[] tokens = tuple.getString(0).split("\\s+");
		/*
		 * TODO: add code to emit all combination of a list of consecutive tokens of length N
		 */
		// BEGIN SOLUTION
		for (int i = 0; i<tokens.length-N+1; i++) {
			StringBuilder builder = new StringBuilder();
			for (int j = 0; j < N; j++) {
				if (builder.length() > 0) builder.append(" ");
				builder.append(tokens[i+j]);
			}
			collector.emit(new Values(builder.toString()));
		}
		// END SOLUTION
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("n-gram"));
	}

}