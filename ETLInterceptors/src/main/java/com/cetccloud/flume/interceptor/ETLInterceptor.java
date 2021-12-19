package com.cetccloud.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ETLInterceptor implements Interceptor {

        private String[] schema; //元数据 id,name,age
        private String separator;//数据的分割符
        private String host;
        private static final Logger logger = LoggerFactory.getLogger(ETLInterceptor.class);
        public  ETLInterceptor(String schema,String separator,Boolean useIp){
            this.separator = separator;
            //字段名以 都逗号分割
            this.schema = schema.split("[,]");
            this.host = null;
            try {
                InetAddress addr = InetAddress.getLocalHost();
                if (useIp) {
                    this.host = addr.getHostAddress();
                } else {
                    this.host = addr.getCanonicalHostName();
                }
            } catch (UnknownHostException var6) {
                logger.warn("Could not get local host address. Exception follows.", var6);
            }
        }

        @Override
        public void initialize() {
            //什么都不做
        }

        @Override
        public Event intercept(Event event) {
            //传递引用，对数据进行修改在传给 events
            Map<String,String> tuple =  new LinkedHashMap<>(); //LinkedhashMap是有序的
            tuple.put("host",host);
            String line = new String(event.getBody());
            StringBuilder stringBuilder = new StringBuilder();
            final String[] datas = line.split(separator);//对传的数据进行切分后再变换成json格式
            for(int i= 1 ;i<=schema.length;i++){
                String key = schema[i - 1];
                String value = " ";
                if (schema.length < datas.length) {
                    if (i == 1) {
                        value = datas[0] +" "+ datas[i];
                    } else {
                        value = datas[i];
                    }

                }
                tuple.put(key,value);
            }
            for (int i = schema.length + 1; i < datas.length; i++) {
                stringBuilder.append(datas[i] + " ");
            }
            tuple.put("content",stringBuilder.substring(0));
            //通过fastJson 把Map类型转换成Json格式的数据
            final String json = JSONObject.toJSONString(tuple);
            //重新复制event的Body
            event.setBody(json.getBytes());
            return event;
        }

        @Override
        public List<Event> intercept(List<Event> events) {  //这个方法是在source传数据 进行批量传输，一条一条传输会直接调用上面的方法
            for(Event e : events){
                intercept(e);//传递的是引用，所以此处的events中的改变了原数据，所以不用接收返回值
            }
            return events;//返回就是把对原始的event进行处理后返回
        }

        @Override
        public void close() {

        }

        /*
        Interceptor.builder的生名周期是
        构造器 -> configure -> builder
         */
        public static class Builder implements Interceptor.Builder{
            private String fields;
            private String separator;
            private Boolean useIp;
            @Override
            public Interceptor build() {
                //在build创建JsonInterceptor 的实例
                return new ETLInterceptor(fields,separator,useIp);
            }

            /*configure 方法 读取配置文件
              配置文件的属性
              1、数据的分割符
              2、字段的名字
              3、数据的分割符
             */
            @Override
            public void configure(Context context) {
                fields = context.getString("fields");
                separator = context.getString("separator"," ");
                useIp = context.getBoolean("useIp",false);
            }
        }
}

