package com.packt.tridentExamples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;

public class FilterTridentTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();
		topology.newDRPCStream("names",drpc)
				.filter(new FilterTrident());
				
		

		Config conf = new Config();
		conf.setDebug(true);


		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident-topology", conf, topology.build());

		for (String word : new String[]{ "Hello" , "Utkarsha", "Prashant"}) {
			System.out.println("Result for " + word + ": " + drpc.execute("names", word));
		}


		cluster.shutdown();
		drpc.shutdown();
	}

}
