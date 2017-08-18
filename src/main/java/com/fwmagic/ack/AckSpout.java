package com.fwmagic.ack;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

/**
 * Created by admin on 2017/8/10.
 */
public class AckSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

//  初始化方法
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //获取数据
    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        collector.emit(new Values(uuid),uuid);
        try {
            //每隔5s发送一次
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //定义发送的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息发送成功："+msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息发送失败："+msgId);
        collector.emit(new Values(msgId),msgId);
    }
}
