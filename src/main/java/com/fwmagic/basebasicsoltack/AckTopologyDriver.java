package com.fwmagic.basebasicsoltack;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by admin on 2017/8/10.
 */
public class AckTopologyDriver {
    public static void main(String[] args) {
      //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("AckSpout",new AckSpout(),1);
        topologyBuilder.setBolt("bolt1",new Bolt1(),1).shuffleGrouping("AckSpout");
        topologyBuilder.setBolt("bolt2",new Bolt2(),1).shuffleGrouping("bolt1");
        topologyBuilder.setBolt("bolt3",new Bolt3(),1).shuffleGrouping("bolt2");
        topologyBuilder.setBolt("bolt4",new Bolt4(),1).shuffleGrouping("bolt3");

        //2、设置任务个数
        Config config = new Config();
        config.setNumWorkers(2);

        StormTopology topology = topologyBuilder.createTopology();
        //本地模式提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("ack",config,topology);
    }
}
