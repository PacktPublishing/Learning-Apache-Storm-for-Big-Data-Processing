package com.packt.stormhadoop;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

import java.io.PrintWriter;
import java.util.Map;

public class WriteTweetsToFile extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map stormConf, TopologyContext context) {

        String fileName = "output"+"-"+context.getThisTaskId()+"-"+context.getThisComponentId()+".txt";
        try{
            this.writer = new PrintWriter(stormConf.get("dirToWrite").toString()+fileName, "UTF-8");
        }catch (Exception e){

        }
    }

    public void execute(Tuple input,BasicOutputCollector collector) {
    	Status status = (Status) input.getValueByField("tweet");
	    String tweetText = status.getText();
	    String tUser = status.getUser().getName();

		collector.emit(new Values(tweetText,tUser));

        writer.println(tweetText+","+tUser);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Tweet","User"));
    }

    public void cleanup() {writer.close();}



}
