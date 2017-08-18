package com.fwmagic.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by admin on 2017/8/10.
 */
public class Bolt3 extends BaseRichBolt {

    private OutputCollector collector;

//    初始化方法，只调用一次
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    //    被循环调用
    @Override
    public void execute(Tuple tuple) {
        collector.emit(tuple,new Values(tuple.getString(0)));
        System.out.println("bolt3的execute方法被调用一次:"+tuple.getString(0));
        collector.ack(tuple);//消息发送成功
//        collector.fail(tuple);//消息发送失败
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }
}
