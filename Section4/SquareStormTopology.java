package com.packt.stormtraining;



import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class SquareStormTopology {

	public static void main(String[] args) throws InterruptedException, Exception{
		
				
		//Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        
        //Loading Spout
        builder.setSpout("Number-Spout", new NumberSpout());
        
        // Spout emitting number ----> Bolt where number is squared
        builder.setBolt("Square-Bolt", new SquareBolt()).shuffleGrouping("Number-Spout");

        // Bolt sq number ---> PrintBolt printing num and sq
        builder.setBolt("Print-Bolt", new PrintBolt()).shuffleGrouping("Square-Bolt");
        
        //Configuration
        Config config = new Config();
        
        //Submit Topology to cluster
        //LocalCluster cluster = new LocalCluster();

       // cluster.submitTopology("square-storm-topology", config, builder.createTopology());
        
        StormSubmitter cluster = new StormSubmitter();
        cluster.submitTopology("square-storm-topology", config, builder.createTopology());
        
        /*
        try {
            Thread.sleep(45000);
        } catch (InterruptedException e) {
        }
        */
        //System.exit(0);
       

	}
}


