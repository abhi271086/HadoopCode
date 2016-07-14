package com.synechron.HadoopStockPoc;

//This class is the starting point for the end-to-end workflow of Intraday data capturing 

import java.io.IOException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class IntraDayStockDataFetcher {
	final static Logger logger = Logger.getLogger(IntraDayStockDataFetcher.class);
	HdfsConnector hdfsReaderWriter = new HdfsConnector();

	public static void main(String[] args) throws IOException

	{
        PropertyConfigurator.configure("log4j.properties");
		IntraDayStockDataFetcher intradayDataFetcher = new IntraDayStockDataFetcher();
		//If today is Monday, first day of the week; then create new URL files else continue with already created ones
		if(StockUtilities.getCurrentWeekMondayDate().equals(StockUtilities.getTodayDate())){
			List<String> validStocks = intradayDataFetcher.getStockList();
			if (!validStocks.isEmpty())
				intradayDataFetcher.urlsToFile(validStocks); 
		}
		FetchDataExecutor dataExecutor = new FetchDataExecutor();
		try {
			dataExecutor.executeDataFetch(StockUtilities.getCurrentWeekMondayDate());
		} catch (Exception e) {
			logger.error(e);
		}
		
		}

	//Get all the valid stock quotes list
	public List<String> getStockList() {
		List<String> stocks = new ArrayList();
		try {

			String stockListFilePath = "/user/hduser/StockPOCData/StockTickerList/valid_stocks";
			stocks = hdfsReaderWriter.read(stockListFilePath);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return stocks;
	}

	public void urlsToFile(List<String> validStocks) {
		// Base URL for all the URLs to be created by addition of valid stock quotes
		String baseURL = "https://query.yahooapis.com/v1/public/yql?format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env&callback=&q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(";
		
		String stockCodeList = "";
		for (int start = 0; start < validStocks.size(); start += 400) {
			int end = Math.min(start + 400, validStocks.size());
			List<String> sublist = validStocks.subList(start, end);
			int leng = sublist.size();
			int i = 0;

			for (String s : sublist) {
				i++;
				if (leng == i && s != "" && s != null && s.length() > 0)
					stockCodeList += s;
				else
					stockCodeList += s + ",";

			}

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			Date date = new Date();
			String fileName = sdf.format(date);
			String finalURL = baseURL + URLEncoder.encode(stockCodeList)+ ")";
			hdfsReaderWriter.write("user/hduser/StockPOCData/StockTickerList/" + fileName, finalURL);
			stockCodeList = "";

		}

	}

}
