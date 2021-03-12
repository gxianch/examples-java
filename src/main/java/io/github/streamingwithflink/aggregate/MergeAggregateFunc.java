 package io.github.streamingwithflink.aggregate;

import java.util.HashMap;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MergeAggregateFunc implements AggregateFunction<Tuple2<String,Integer>, HashMap<String, Integer>, HashMap<String, Integer>> {

	@Override
	public HashMap<String, Integer> createAccumulator() {
		// TODO Auto-generated method stub
		 return new HashMap<>();
	}

	@Override
	public HashMap<String, Integer> add(Tuple2<String, Integer> value, HashMap<String, Integer> accumulator) {
		if(accumulator.containsKey(value.f0)) {
			accumulator.put(value.f0, accumulator.get(value.f0)+value.f1);
		}else {
			accumulator.put(value.f0, value.f1);
		}
		 return accumulator;
	}

	@Override
	public HashMap<String, Integer> getResult(HashMap<String, Integer> accumulator) {
		// TODO Auto-generated method stub
		 return accumulator;
	}

	@Override
	public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
		a.putAll(b);
		 return a;
	}

	
	

}
