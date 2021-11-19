package com.packt.tridentExamples;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class JoinExampleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    Random random = new Random();


    public void ack(Object msgId) {}


    public void fail(Object msgId) {}

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;
    }


    public void nextTuple() {


            collector.emit(new Values(random.nextInt(100),random.nextInt(100)));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("x1","x2"));
    }


}
