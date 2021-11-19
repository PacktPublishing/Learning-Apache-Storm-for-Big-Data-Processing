package com.packt.stormtraining;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt{

	private HashMap<Integer, Integer> numsq = null;
	
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Integer num = tuple.getIntegerByField("number");
        Integer res = tuple.getIntegerByField("numbersquare");
        this.numsq.put(num, res);
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {

		numsq = new HashMap<Integer,Integer>();
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		System.out.println("--- Results by Program ---");
        List<Integer> keys = new ArrayList<Integer>();
        keys.addAll(this.numsq.keySet());
        Collections.sort(keys);
        for (Integer key : keys) {
            System.out.println(key + " : " + this.numsq.get(key));
        }
        System.out.println("--------------");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
