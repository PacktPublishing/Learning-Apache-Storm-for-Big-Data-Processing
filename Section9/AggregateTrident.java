package com.packt.tridentExamples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class AggregateTrident {
	public static void main(String[] args) {
		
		
		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();
		topology.newDRPCStream("simple", drpc)
				.map(new MapTridentExample())
				.groupBy(new Fields("args"))
				.aggregate(new Count(), new Fields("count"));
		

		Config conf = new Config();
		conf.setDebug(true);


		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident-topology", conf, topology.build());

		for (String word : new String[]{ "hello" , "welcome"}) {
			System.out.println("Result for " + word + ": " + drpc.execute("simple", word));
		}
		
		cluster.shutdown();
		drpc.shutdown();

	}

}
