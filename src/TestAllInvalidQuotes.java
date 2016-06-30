import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;


public class TestAllInvalidQuotes {
public static void main(String[] args) throws IOException {
	String invalidQuoteList[] = {"TFSCR","TFSCW","ABEOW","AITP","ADXSW","AGFSW","AMBCW","AGNCP","ASRVP","IBUY","ANDAR","ANDAW","ZLIG","AMEH","APDNW","AUMAU","AUMAW","AGIIL","ARWAR","ARWAU","ARWAW","DWAT","PUMP","AGMX","BHACR","BHACU","BHACW","BNTCW","BOLT","BVXVW","ITEQ","BOFIL","BPFHP","BPFHW","BLVDU","BLVDW","BOXL","BMLA","CLACW","CAPNW","CATYW","CELGZ","CLRBW","CLRBZ","CERCW","CERCZ","CFCOU","HOTRW","CHEKW","CHSCL","CIVBP","CBMXW","CCFI","CYHHZ","CONG","CFRXW","COYNW","COWNL","CYRXW","CYTXW","TACOW","DELTW","EAGLU","EAGLW","CADTR","CADTU","CADTW","EACQU","EACQW","EVGBC","EVSTC","EVLMC","EBAYL","ECACR","ECACU","ELECW","CAPX","ESSF","EYEGW","FSCFL","FNTCU","FNTCW","FFBCW","FAAR","FVC","FEMB","FPXI","LMBS","CIBR","RFAP","RFDI","RFEM","RFEU","FCVT","LKOR","ASET","QLC","FTRPR","FULLL","GALTU","GALTW","GGACR","GGACW","GCTS","GEMP","GFNCP","GFNSL","GNST","GOODM","GOODN","GAINN","GAINO","GAINP","GBLIZ","GPACU","GPACW","ACTX","BFIT","LNGR","MILN","CATH","ALTY","GRSHU","GRSHW","GPIAU","GPIAW","HBHCL","HRMNU","HRMNW","HCAPL","HCACU","HCACW","HBANP","HDRAR","HDRAU","HDRAW","IBKCO","IBKCP","ICLDW","INTLL","FALN","HYXE","MPCT","JSML","JSMD","JASNW","JSYNR","JSYNU","JSYNW","WYIGW","KTOVW","KLREU","KLREW","DRIOW","LCAHU","LMRKP","DDBI","EDBI","LVHD","UDBI","BATRA","LMCA","LSXMA","LSXMB","LINDW","LMFAW","CNCR","LTEA","MIII","MIIIU","MAPI","MFINL","MDVXW","MICTW","MINDP","NGHCO","NGHCZ","NBCP","NUROW","NSIG","NYMTO","NYMTP","NEWTL","NEWTZ","NXEOU","NXEOW","NORT","NWBOW","NXTDW","OCLSW","OSBCP","ONSIW","ONSIZ","OPXAW","OPGNW","OACQR","OACQU","OACQW","OTG","OXBRW","OXLCO","PACEU","PACEW","PAACR","PAACU","PAACW","PAVMU","PLXP","PMV","BPOPM","PDBC","DWIN","DWTR","IDLB","USLB","PSET","PY","PVTBP","QPACU","QPACW","RETA","UTES","TAPR","RNVAW","RSAS","RBIO","RIBTW","RITTW","TALL","SANW","GCVRZ","SBFGP","SHLDW","SCCI","SBNYW","SRAQW","SRVA","SPU","ISM","JSM","OSM","SOHOL","SOHOM","SPTN","DWFI","SPSC","SBLKL","SRCLP","SNDE","SPRT","SIVBO","SGYPW","TCMD","TRTLW","TCBIL","TCBIW","FITS","OLD","SLIM","ORG","BITE","TMUSP","TROVU","TROVW","UREE","GNRX","VIGI","VYMI","VNRAP","VNRCP","VMET","CIZ","CEZ","CID","CIL","CDL","CSB","VIDI","VKTXW","WAFDW","WNFM","WHLRW","WHFBL","WVVIP","WTFCM","WTFCW","WMGIZ","WSFSL","XGTIW","ZNWAA","ZIONW","ZIONZ"};
	String url = "https://finance.google.com/finance/info?client=ig&q=NASDAQ:";
	URL STREAM_URI;
	HttpURLConnection connection;
	
	int code =0;

	 
	 
	 BufferedReader reader ;
	
	 for(int i=0;i<invalidQuoteList.length;i++){
		 STREAM_URI = new URL(url+invalidQuoteList[i]);	 
		 connection = (HttpURLConnection)STREAM_URI.openConnection();
		 connection.setRequestMethod("GET");
			connection.connect();
			
		 System.out.println( "Stock Number:" + i + "Stock name: " + invalidQuoteList[i] + " " +    connection.getResponseCode());
	 }
	

	/*int code = connection.getResponseCode();
	 HdfsTest hdfsWrite = new HdfsTest();
	 hdfsWrite.write("KoushikCheck","I am HDFS Writer" );*/
}
	
	
	
}
