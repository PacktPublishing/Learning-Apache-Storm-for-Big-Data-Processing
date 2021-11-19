package com.packt.stormstreamgrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class DirectGroupingImpl {
public static void main(String[] args) throws Exception {

    //Topology definition
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("NumberSpout", new DirectGroupingSpoutExample());
    
    builder.setBolt("FileWriterBolt", new FileWriterBolt(),2)
           .directGrouping("NumberSpout");

    //Configuration
    Config conf = new Config();
    conf.setDebug(true);
    conf.put("dirToWrite", "data_output/direct_output");
    
    //Topology run
    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
    LocalCluster cluster = new LocalCluster();
    try{
        cluster.submitTopology("Direct-Grouping-Topology", conf, builder.createTopology());
        Thread.sleep(10000);
    }
    finally{
    cluster.shutdown();}
    }
}
