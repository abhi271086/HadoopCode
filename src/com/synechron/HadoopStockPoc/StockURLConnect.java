package com.synechron.HadoopStockPoc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.CDL;
import org.json.JSONArray;

public class StockURLConnect implements Runnable {

	private final String stockURL;

	StockURLConnect(String stockURL) {
		this.stockURL = stockURL;
	}

	@Override
	public void run() {

		while (true) {
			try {
				URL stockDataURL = new URL(stockURL);
				HttpURLConnection connection = (HttpURLConnection) stockDataURL.openConnection();
				connection.setRequestMethod("GET");
				connection.connect();
				BufferedReader br = new BufferedReader(new InputStreamReader((connection.getInputStream())));
				String line;
				String completeMinuteQuoteData = "";
				while ((line = br.readLine()) != null) {
					completeMinuteQuoteData += line;
				}
				completeMinuteQuoteData = completeMinuteQuoteData.replace("// ", "");
				JSONArray output;
				output = new JSONArray(completeMinuteQuoteData);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				Date date = new Date();
				String fileName = sdf.format(date);

				String csv = CDL.toString(output);
				HdfsConnector hdfsWriter = new HdfsConnector();
				hdfsWriter.write("/user/hduser/StockPOCData/intraDayStockData/" + fileName, csv);
				Thread.sleep(120000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
