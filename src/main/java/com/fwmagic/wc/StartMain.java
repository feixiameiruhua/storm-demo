package com.fwmagic.wc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by admin on 2017/8/18.
 */
public class StartMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new MySpout());
        builder.setBolt("bolt1",new MyBolt()).shuffleGrouping("spout");
        builder.setBolt("bolt2",new MyWCAndPrintBolt()).shuffleGrouping("bolt1");

        Config config = new Config();
        config.setNumWorkers(1);

        StormTopology topology = builder.createTopology();
        if(args!=null && args.length>0){
            StormSubmitter.submitTopology("wordcount",config,topology);
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcount",config,topology);
        }
    }
}
