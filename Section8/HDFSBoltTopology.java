package com.packt.stormhadoop;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

public class HDFSBoltTopology {
	public static void main(String[] args)throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new TwitterConnectSpout());
		
		builder.setBolt("tweet-to-file", new WriteTweetsToFile())
				.shuffleGrouping("tweets-collector");
//=================================================================		
		
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        //Rotate files after 127MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(127.0f, FileSizeRotationPolicy.Units.MB);

        DefaultFileNameFormat fileNameFormat = new DefaultFileNameFormat();

        //The files are written in this HDFS folder
        fileNameFormat.withPath("/user/cloudera/mytweets_storm");

        //Files start with the following filename prefix
        fileNameFormat.withPrefix("record-");

        //Files end with the following suffix
        fileNameFormat.withExtension(".csv");

        //HDFS bolt
        HdfsBolt hdfsbolt = new HdfsBolt().withFsUrl("hdfs://192.168.247.140:8020")
                        .withFileNameFormat(fileNameFormat)
                        .withRecordFormat(format)
                        .withRotationPolicy(rotationPolicy)
                        .withSyncPolicy(syncPolicy);
		
		
//=====================================================================		
		
        builder.setBolt("HDFS-Bolt", hdfsbolt).shuffleGrouping("tweet-to-file");
		
		

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
