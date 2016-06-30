#!/bin/bash
cd /root/StockPocData/scripts/

csvCount=`ls -l *.csv | wc -l`
todayDate=`date +%Y%m%d`

if [ $csvCount ] 
	then
	`rm -f *.csv valid_stocks`
fi

wget 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download' -O `echo $todayDate`_stocks.csv

awk -F'","|^"|"$'  '{if ($5 > 0 && NR>1) print $2}' `echo $todayDate`_stocks.csv > `echo $todayDate`_validStocks.csv
cp `echo $todayDate`_validStocks.csv valid_stocks

hdfs dfs -rm valid_stocks
hdfs dfs -put `echo $todayDate`_validStocks.csv /user/hduser/StockPOCData/StockTickerList/archive
hdfs dfs -put valid_stocks /user/hduser/StockPOCData/StockTickerList/

rm -f *.csv
