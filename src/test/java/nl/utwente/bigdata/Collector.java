package nl.utwente.bigdata;

import java.util.Collection;

import java.util.List;

import java.util.LinkedList;
import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

class Collector implements IOutputCollector {
	public List<List<Object>> output = new LinkedList<List<Object>>();
	
	@Override
	public void reportError(Throwable error) {
	}

	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors,
			List<Object> tuple) {
		output.add(tuple);
		return null;
	}

	@Override
	public void emitDirect(int taskId, String streamId,
			Collection<Tuple> anchors, List<Object> tuple) {
		//System.out.println(tuple);
	}

	@Override
	public void ack(Tuple input) {
		//
	}

	@Override
	public void fail(Tuple input) {
		// TODO Auto-generated method stub
		
	}
	
}