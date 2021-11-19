package com.packt.tridentExamples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowTridentExample {
	public static void main(String[] args) throws Exception {

		Logger LOG = LoggerFactory.getLogger(WindowTridentExample.class);

		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);


		WindowsStoreFactory windowStore = new InMemoryWindowsStoreFactory();

		WindowConfig windowConfig = SlidingCountWindow.of(100, 10);


		TridentTopology topology = new TridentTopology();

		Stream stream = topology.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("words"))
				.window(windowConfig, windowStore, new Fields("words"),
						new CountAsAggregator(), new Fields("count"))
				.peek(new Consumer() {
					
					@Override
					public void accept(TridentTuple arg0) {
						// TODO Auto-generated method stub
						
						LOG.info("Received tuple: [{}]", arg0);
						
					}
				})
				;
		
		
		

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
