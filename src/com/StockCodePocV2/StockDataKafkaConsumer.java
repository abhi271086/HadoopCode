package com.synechron.HadoopStockPoc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class StockDataKafkaConsumer extends Thread{

	boolean isKafkaConsumerRunning=false; 
	
	public boolean isKafkaConsumerRunning() {
		return isKafkaConsumerRunning;
	}

	public void setKafkaConsumerRunning(boolean isKafkaConsumerRunning) {
		this.isKafkaConsumerRunning = isKafkaConsumerRunning;
	}

	/*		public static boolean getIsKafkaConsumerRunning() {
		return StockDataKafkaConsumer.isKafkaConsumerRunning;
	}

	public static void setIsKafkaConsumerRunning(boolean isKafkaConsumerRunning) {
		StockDataKafkaConsumer.isKafkaConsumerRunning = isKafkaConsumerRunning;
	}

	public StockDataKafkaConsumer() {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());

	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "Hadoop2.synechron.com:2181");
		props.put("group.id", "StockData");
		props.put("zookeeper.session.timeout.ms", "9000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);

	}
*/
	@Override
	public void run() {
		try {
			isKafkaConsumerRunning=true;
			String topic = "StockDataKafka";
			Configuration configuration = new Configuration();
			
			Properties props = new Properties();
			props.put("zookeeper.connect", "Hadoop2.synechron.com:2181");
			props.put("group.id", "StockData");
			props.put("zookeeper.session.timeout.ms", "9000");
			props.put("zookeeper.sync.time.ms", "200");
			props.put("auto.commit.interval.ms", "1000");
			ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
			

			HdfsConnector hdfsWriteKafkaToHDFS = new HdfsConnector(); 
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			System.out.println("Started writing kafka topic data to hdfs file");
			while (it.hasNext()) {
				// call the write method and pass the single line AS string
				String msg = new String (it.next().message(), "UTF-8");
				System.out.println(msg);
				hdfsWriteKafkaToHDFS.write("user/hduser/StockPOCData/intraDayStockData/" + StockUtilities.getTodayDate(),
						msg);
			}
			isKafkaConsumerRunning=false;
		}

		catch (Exception e1) {
			e1.printStackTrace();
		}
				
	}
}