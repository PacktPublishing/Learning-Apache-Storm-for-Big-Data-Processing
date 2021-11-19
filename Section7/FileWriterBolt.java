package com.packt.stormstreamgrouping;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class FileWriterBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map stormConf, TopologyContext context) {

    	//Initialize the PrintWriter method to write integers in the file
    	//Each bolt of the same class will get the different componentID which we will
    	//use in this program to seperate integers
        String fileName = "output"+"-"+context.getThisTaskId()+"-"+context.getThisComponentId()+".txt";
        try{
            this.writer = new PrintWriter(stormConf.get("dirToWrite").toString()+fileName, "UTF-8");
        }catch (Exception e){

        }
    }

    public void execute(Tuple input,BasicOutputCollector collector) {
        String str = input.getStringByField("integer")+"-"+input.getStringByField("bucket");
        collector.emit(new Values(str));
        writer.println(str);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }

    public void cleanup() {
    	writer.close();
    }



}
