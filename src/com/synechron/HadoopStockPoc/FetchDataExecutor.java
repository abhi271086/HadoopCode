package com.synechron.HadoopStockPoc;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FetchDataExecutor {

	public static void executeDataFetch(String mondayDate) throws Exception {

		HdfsConnector hdfsReadWrite = new HdfsConnector();
		List<String> urlList = hdfsReadWrite.read("user/hduser/StockPOCData/StockTickerList/" + mondayDate);
		final int MYTHREADS = urlList.size();

		ExecutorService executor = Executors.newFixedThreadPool(MYTHREADS);
		for (int i = 0; i < urlList.size(); i++) {

			String url = urlList.get(i);
			Runnable worker = new StockURLConnect(url);
			executor.execute(worker);
		}
		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");
	}
}
