package com.packt.tridentExamples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MapTridentTopology {

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		

		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();
		topology.newDRPCStream("names",drpc)
				.map(new MapTridentExample());
				
		

		Config conf = new Config();
		conf.setDebug(true);


		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident-topology", conf, topology.build());

		for (String word : new String[]{ "Prashant" , "Utkarsha", "Prem"}) {
			System.out.println("Result for " + word + ": " + drpc.execute("names", word));
		}


		cluster.shutdown();
		drpc.shutdown();

	}

}
