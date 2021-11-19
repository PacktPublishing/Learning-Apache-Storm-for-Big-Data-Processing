package com.packt.stormhadoop;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterExampleTopology {
	
	public static void main(String[] args)throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new TwitterConnectSpout());
		
		builder.setBolt("tweet-to-file", new WriteTweetsToFile())
				.shuffleGrouping("tweets-collector");
		
		//Configuration & Topology Startup
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "twitter-data\\myoutput");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("twitter-test", conf, builder.createTopology());
		Thread.sleep(60000);
	cluster.shutdown();
	}

}
