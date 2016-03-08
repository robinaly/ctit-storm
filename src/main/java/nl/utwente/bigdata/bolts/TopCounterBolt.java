package nl.utwente.bigdata.bolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nl.utwente.bigdata.util.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopCounterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 394263766896592119L;
	private Map<Object, Integer> counts = new HashMap<Object, Integer>();
	private long N = 3;
	private long interval = 10;
	
	private final class ValueComparator<V extends Comparable<? super V>>
                                     implements Comparator<Map.Entry<?, V>> {
		public int compare(Map.Entry<?, V> o1, Map.Entry<?, V> o2) {
			return -1 * o1.getValue().compareTo(o2.getValue());
		}
	}
	
	@Override
	public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
		if (stormConf != null && stormConf.containsKey("N")) {
			N = Long.parseLong((String) stormConf.get("N"));
		}
		if (stormConf != null && stormConf.containsKey("interval")) {
			interval = Long.parseLong((String) stormConf.get("interval"));
		}
	};
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (TupleHelpers.isTickTuple(tuple)) {
			// this was a chronological tickle tuple - generate output
			// BEGIN SOLUTION
			final int size = counts.size();
		    final List<Map.Entry<Object, Integer>> list = new ArrayList<Map.Entry<Object, Integer>>(size);
		    list.addAll(counts.entrySet());
		    final ValueComparator<Integer> cmp = new ValueComparator<Integer>();
		    Collections.sort(list, cmp);
		    for (int i = 0; i < Math.min(N, size); i++) {
		    	Map.Entry<Object, Integer> e = list.get(i);
		    	collector.emit(new Values(e.getKey(), e.getValue())); 
		    }		    
			counts.clear();
			// END SOLUTION
		} else {
			// this was a real tuple - add to input
			// BEGIN SOLUTION
			Object value = tuple.getValue(0);
			Integer count = counts.get(value);
			if (count == null) count = 0;
			counts.put(value, count+1);
			// END SOLUTION
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
		return conf;
	}
}