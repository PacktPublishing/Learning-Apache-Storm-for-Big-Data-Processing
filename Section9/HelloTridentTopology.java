package com.packt.tridentExamples;


import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;

public class HelloTridentTopology {

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		
	
		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();
		topology.newDRPCStream("names",drpc)
				.each(new Fields("args"),
						new HelloTrident(),
						new Fields("greeting"));

		Config conf = new Config();
		conf.setDebug(true);


		LocalCluster cluster = new LocalCluster();
		
		try
		{
		cluster.submitTopology("trident-topology", conf, topology.build());

		for (String word : new String[]{ "Prashant" , "Utkarsha"}) {
			System.out.println("Result for " + word + ": " + drpc.execute("names", word));
		}

		}
		catch(Exception e) {

		cluster.shutdown();


		drpc.shutdown();
		
		}
	}

}
