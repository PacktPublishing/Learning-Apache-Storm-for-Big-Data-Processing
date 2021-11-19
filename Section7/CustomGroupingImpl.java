package com.packt.stormstreamgrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class CustomGroupingImpl {

	public static void main(String[] args)throws Exception {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Emit_Number", new NumberSpout());
        
        //Implementing custom grouping
        builder.setBolt("File_Write_Bolt", new FileWriterBolt(),2)
        	   .customGrouping("Emit_Number", new ManualGrouping());

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "data_output/custom_output");
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Custom-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally{
        cluster.shutdown();}
        }

}
