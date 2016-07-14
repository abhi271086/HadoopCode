package com.synechron.HadoopStockPoc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.CDL;
import org.json.JSONArray;

public class StockURLConnect implements Runnable {

	private final String stockURL;
	private int threadNum=0;
	static StockDataKafkaConsumer kafkaConsumeToHDFS = new StockDataKafkaConsumer();
	StockURLConnect(String stockURL, int threadNum) {
		System.out.println(stockURL);
		this.stockURL = stockURL;
		this.threadNum = threadNum;
	}

	@Override
	public void run() {
		
		while (true) {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(((HttpURLConnection) (new URL(stockURL)).openConnection()).getInputStream(), Charset.forName("UTF-8")));
				StringBuilder line;
				StringBuilder completeMinuteQuoteData = new StringBuilder("");
				completeMinuteQuoteData = new StringBuilder(br.readLine());
				/*while ((line = new StringBuilder(br.readLine()))!= null) {
					completeMinuteQuoteData.append(line) ;
				}*/
				System.out.println(completeMinuteQuoteData);
				completeMinuteQuoteData = completeMinuteQuoteData.replace(0,completeMinuteQuoteData .indexOf("[{\"symbol\""),"").replace(completeMinuteQuoteData .indexOf("}}}"), completeMinuteQuoteData.length(), "");
				JSONArray output = new JSONArray(completeMinuteQuoteData.toString());
				//SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				//Date date = new Date();
				//String fileName = sdf.format(date) + "_" + threadNum;

				String csv = CDL.toString(output);
				//HdfsConnector hdfsWriter = new HdfsConnector();
				StockDataPublisher.pushToKafkaTopic(csv);
				
				if(!kafkaConsumeToHDFS.isKafkaConsumerRunning()){
					System.out.println("Before thread started");
					kafkaConsumeToHDFS.start();
					System.out.println("After thread started");
				}
				//hdfsWriter.write("/user/hduser/StockPOCData/intraDayStockData/" + fileName, csv);
				Thread.sleep(120000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

