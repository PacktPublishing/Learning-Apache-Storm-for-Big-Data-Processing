package com.packt.stormstreamgrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class FieldGroupingImpl {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Emit_Number", new NumberSpout());
        
        //In this example we will see how to control the stream.
        //What we will do here is we will try to put all the integers
        //that belong to one bucket in one bolt (Technically one file)
        //This is where FieldGrouping help
        
        builder.setBolt("File_Write_Bolt", new FileWriterBolt(),2)
        		.fieldsGrouping("Emit_Number", new Fields("bucket"));
        		              //Spout, seperation based on ?

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "data_output/field_output");
        
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Fields-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally{
        cluster.shutdown();}
        }
	
	//In terms of its application, its very useful, <statewise data> etc.

	}


