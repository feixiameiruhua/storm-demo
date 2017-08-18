package com.fwmagic.wc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/8/18.
 */
public class MyWCAndPrintBolt extends BaseBasicBolt {
    //添加序列化id
    private static final long serialVersionUID = 2006509824957897376L;
    private Map<String,Integer> map = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = (String) tuple.getValueByField("word");
        Integer num = Integer.parseInt((String)tuple.getValueByField("num"));
        Integer oldNum = map.get(word);
        if(oldNum!=null){
            map.put(word,oldNum+num);
        }else{
            map.put(word,num);
        }
        System.out.println(map);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
