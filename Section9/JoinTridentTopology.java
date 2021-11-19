package com.packt.tridentExamples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;


public class JoinTridentTopology {

	public static void main(String[] args) throws Exception {


		TridentTopology topology = new TridentTopology();

		Stream source = topology.newStream("spout1", new JoinExampleSpout());

        Stream sum = source
				       .each(new Fields("x1", "x2"), new AddFunction(), new Fields("sum"));

		Stream product = source
				.each(new Fields("x1", "x2"), new MultiplyFunction(), new Fields("product"));


		topology.merge(sum,product)
				.peek(new Consumer() {
					public void accept(TridentTuple tridentTuple) {
						System.out.println("From merge stream:"+tridentTuple);
					}
				});

		topology.join(sum,new Fields("x1","x2"),product,new Fields("x1","x2"),
                new Fields("x1","x2","sum","product"))
				.peek(new Consumer() {
					public void accept(TridentTuple tridentTuple) {
						System.out.println("From join stream:" + tridentTuple);
					}
				});;


		Config conf = new Config();
		conf.setDebug(true);


		LocalCluster cluster = new LocalCluster();

		try{
			cluster.submitTopology("trident-topology", conf, topology.build());
			Thread.sleep(15000);
		}
		finally {
			cluster.shutdown();
		}

	}
}
