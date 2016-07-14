package com.synechron.HadoopStockPoc;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class StockUtilities {

	public static String getCurrentWeekMondayDate(){
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(c.getTime());
	}
	
	public static String getTodayDate(){
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(c.getTime());
	}
}
