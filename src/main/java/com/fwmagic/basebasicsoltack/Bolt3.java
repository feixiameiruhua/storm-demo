package com.fwmagic.basebasicsoltack;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by admin on 2017/8/10.
 */
public class Bolt3 extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("bolt3的execute方法被调用一次:"+tuple.getString(0));
//        basicOutputCollector.emit(new Values(tuple.getString(0)));
        throw  new FailedException("网络错误，请重试！");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }
}
