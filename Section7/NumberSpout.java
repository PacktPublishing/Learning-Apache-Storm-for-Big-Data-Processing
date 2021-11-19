package com.packt.stormstreamgrouping;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class NumberSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private Integer i = 0;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;
    }


    public void nextTuple() {

        while(i<=100){

        	//here it will decide the integer belongs to which bucket
        	// e.g. 0-9 will be in bucket0 and 10-19 will be in bucket1 and so on
            Integer intBucket = (this.i/10);


            this.collector.emit(new Values(this.i.toString(),intBucket.toString()));
            this.i = this.i + 1;

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer","bucket"));
    }


}
