package com.packt.stormstreamgrouping;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;



public class LogEmitterSpout extends BaseRichSpout {

	 private SpoutOutputCollector collector;

	    private static final Integer MAX_PERCENT_FAIL = 80;
	    Random random = new Random();


	    public void ack(Object msgId) {}


	    public void fail(Object msgId) {}

	    public void open(Map conf, TopologyContext context,
	                     SpoutOutputCollector collector) {

	        this.collector = collector;
	    }


	    public void nextTuple() {

	        Integer r = random.nextInt(100);
	        if(r < MAX_PERCENT_FAIL){
	            collector.emit(new Values("Success"));

	        }else{
	            collector.emit(new Values("ERROR"));
	        }


	    }

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {

	        declarer.declare(new Fields("log"));
	    }
	
}
