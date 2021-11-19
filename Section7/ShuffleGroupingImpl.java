package com.packt.stormstreamgrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class ShuffleGroupingImpl {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Emit_Number", new NumberSpout());
        
        //Note: we are specifying how much instances of bolt must be instantiated. e.g. 2
        //The distribution strategy of the data is decided by the grouping method used
        //Here in this case we are using shuffleGrouping(default) which randomly picks the bolt  
        //instance to send the data.
        
        builder.setBolt("File_Write_Bolt", new FileWriterBolt(),2)
        .shuffleGrouping("Emit_Number");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "data_output\\shuffle_output");
        
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Shuffle-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally{
        cluster.shutdown();}
        }

	}


