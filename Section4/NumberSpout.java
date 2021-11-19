package com.packt.stormtraining;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class NumberSpout extends BaseRichSpout{
	private SpoutOutputCollector collector;
	private Integer i = new Integer(2);
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		//Emit only even numbers
		this.collector.emit(new Values(i));
		try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

		i+=2;
		
		
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("numbers"));
		
	}

}
