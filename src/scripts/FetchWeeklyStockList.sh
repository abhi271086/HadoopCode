#!/bin/ksh
cd /root/StockPocData/scripts/

csvCount=`ls -l *.csv | wc -l`

if [ $csvCount > 0 ] 
	then
	`rm -f *.csv`
fi

wget 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download' -O stocks.csv

awk -F'","|^"|"$'  '{if ($5 > 0 && NR>1) print $2}' stocks.csv > `date +%Y%m%d.csv`

rm -f stocks.csv

hadoop dfs -mv /user/hduser/StockPOCData/StockTickerList/*.csv /user/hduser/StockPOCData/StockTickerList/archive/
hadoop dfs -put *.csv /user/hduser/StockPOCData/StockTickerList/
