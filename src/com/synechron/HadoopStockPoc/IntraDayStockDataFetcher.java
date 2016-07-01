package com.synechron.HadoopStockPoc;

//This class is the starting point for the end-to-end workflow of Intraday data capturing 

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class IntraDayStockDataFetcher {
	HdfsConnector hdfsReaderWriter = new HdfsConnector();

	public static void main(String[] args) throws IOException

	{
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
			e.printStackTrace();
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
		String baseURL = "https://finance.google.com/finance/info?client=ig&q=NASDAQ:";
		String stockCodeList = "";
		for (int start = 0; start < validStocks.size(); start += 100) {
			int end = Math.min(start + 100, validStocks.size());
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
			hdfsReaderWriter.write("user/hduser/StockPOCData/StockTickerList/" + fileName, baseURL + stockCodeList);
			stockCodeList = "";

		}

	}

}
