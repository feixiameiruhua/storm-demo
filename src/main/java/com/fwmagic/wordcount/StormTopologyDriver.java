package com.fwmagic.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by admin on 2017/8/7.
 */
public class StormTopologyDriver {//

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MyLocalFileSpout());
        topologyBuilder.setBolt("bolt1", new MySplitBolt()).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("bolt2", new MyWordCountAndPrintBolt()).shuffleGrouping("bolt1");

        //2、任务提交
        //提交给谁？提交什么内容？
        Config config = new Config();
//        config.setNumWorkers(2);
        StormTopology stormTopology = topologyBuilder.createTopology();
//        //本地模式
        if (args != null && args.length > 0) {
            //集群模式
            StormSubmitter.submitTopology("wordcount", config, stormTopology);
        } else {
            //本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcount", config, stormTopology);
//            localCluster.shutdown();
        }
    }
}
