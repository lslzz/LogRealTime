package com.cetecloud.logalarm.beans;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author lslzz
 * 自定义反序列化类
 */
public class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");
        //获取库名
        String db = sourceStruct.getString("db");
        //获取表名
        String table = sourceStruct.getString("table");

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String opStr = operation.toString().toLowerCase();
        //做一个类型修正
        if ("create".equals(opStr)){
            opStr="insert";
        }

        JSONObject jsonObj = new JSONObject();
        JSONObject dataJsonObj = new JSONObject();
        //获取数据，封装成json
        Struct afterStruct = valueStruct.getStruct("after");
        if(afterStruct!=null){
            List<Field> fields = afterStruct.schema().fields();
            for (Field field : fields) {
                String fieldName = field.name();
                Object value = afterStruct.get(field);
                dataJsonObj.put(fieldName, value);
            }
        }


        jsonObj.put("database", db);
        jsonObj.put("table", table);
        jsonObj.put("data", dataJsonObj);
        jsonObj.put("type", opStr);

        collector.collect(jsonObj.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
