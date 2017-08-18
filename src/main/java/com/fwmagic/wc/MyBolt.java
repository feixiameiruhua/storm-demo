package com.fwmagic.wc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by admin on 2017/8/18.
 * 自定义MyBolt
 */
public class MyBolt extends BaseBasicBolt {
    //添加序列化id
    private static final long serialVersionUID = 317153879824022121L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = (String)tuple.getValueByField("wc");
        String[] arr = line.split(",");
        for (String word : arr) {
            basicOutputCollector.emit(new Values(word,"1"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","num"));
    }
}
