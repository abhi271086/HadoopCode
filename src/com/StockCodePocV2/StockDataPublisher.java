package com.synechron.HadoopStockPoc;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class StockDataPublisher {

	static ProducerConfig config ;
	static Producer<String, String> producer;
	static KeyedMessage<String, String> data;
	static{
		Properties props = new Properties();
	    props.put("metadata.broker.list", "Hadoop2.synechron.com:9092");
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
	    props.put("request.required.acks", "1");
	    props.put("retry.backoff.ms", "150");
	    props.put("message.send.max.retries","10");
	    props.put("topic.metadata.refresh.interval.ms","0");
	    props.put("group.id","StockData");
	    config = new ProducerConfig(props);
    	producer = new Producer<String, String>(config); 
    	 
	}
    public static void pushToKafkaTopic(String line){
    	System.out.println(line);
    	data = new KeyedMessage<String, String>("StockDataKafka",line);
   	 	try{
   	 	System.out.println("producer is "+ producer.toString());
   	 	producer.send(data);	
   	 	}
    	catch(Exception e){
    		e.printStackTrace();
    	}
    }

}
