package com.cetecloud.logalarm.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.Map;

public class MySourceFunction extends ProcessFunction<String, JSONObject> {
    public static String topic = null;
    public MySourceFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void processElement(String str, Context ctx, Collector<JSONObject> out) throws Exception {
        Map<String, String> tuple = new LinkedHashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        String[] data = str.split(" ");
        tuple.put("host",data[0]);
        if (data.length > 3) {
            String date = (data[1] + " " + data[2]).split(",")[0];

            if ("[".equals(date.substring(0,1))) {
                tuple.put("date",date.substring(1));
            } else {
                tuple.put("date",date);
            }
            tuple.put("level",data[3]);
        }

        if (topic != null) {
            tuple.put("service",topic.split("_")[0]);
        }
        for (int i = tuple.size(); i < data.length; i++) {
            stringBuilder.append(data[i] + " ");
        }

        tuple.put("info",stringBuilder.substring(0));


        String jsonStr = JSONObject.toJSONString(tuple);

        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        out.collect(jsonObject);
    }
}
