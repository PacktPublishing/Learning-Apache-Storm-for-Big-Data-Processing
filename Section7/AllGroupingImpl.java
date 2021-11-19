package com.packt.stormstreamgrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class AllGroupingImpl {

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		//Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Emit_Number", new NumberSpout());
        
        //It ensure every stream emitted will go to all the bOlt instances
        
        builder.setBolt("File_Write_Bolt", new FileWriterBolt(),2)
        	   .allGrouping("Emit_Number");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "data_output/all_output");
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("All-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally{
        cluster.shutdown();}
        }

	}


