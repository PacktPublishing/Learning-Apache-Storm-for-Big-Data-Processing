package com.packt.stormstreamgrouping;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.List;


public class DirectGroupingSpoutExample extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private Integer i = 0;

    private List<Integer> boltIds;

    public void ack(Object msgId) {}


    public void fail(Object msgId) {}

    //Get the bolt Ids 
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;
        this.boltIds = context.getComponentTasks("FileWriterBolt");
    }

    //Allows you to apply grouping logic in spout itself
    public void nextTuple() {

        while(i<=100){


            Integer intBucket = (this.i/10);


            this.collector.emitDirect(boltIds.get(getBoltId(intBucket)), new Values(this.i.toString(), intBucket.toString()));
            this.i = this.i + 1;

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer","bucket"));
    }


    public Integer getBoltId(Integer intBucket){

        return intBucket % boltIds.size();

    }


}
