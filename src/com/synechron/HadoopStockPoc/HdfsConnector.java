package com.synechron.HadoopStockPoc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsConnector {
	StringBuilder data = new StringBuilder();
	String hdfsLoc;
	Configuration conf = new Configuration();
	List<String> stockCode = new ArrayList<>();
	String fileLoc = "";
	String stockName;
	String stockFromFile = "";
	FileSystem fs;
	public HdfsConnector(){
		conf.set("fs.defaultFS", "hdfs://Hadoop2.synechron.com:8020/user");
		conf.set("hadoop.job.ugi", "root");
	
	}

	public void write(String location, final String line) {
		try {
			fs = FileSystem.get(conf);
			fileLoc = location;
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					String finalDataToWrite = line;
					Path path = new Path("hdfs://Hadoop2.synechron.com:8020/" + fileLoc);
					finalDataToWrite = finalDataToWrite.replaceFirst(".*\\n", "");
					finalDataToWrite = finalDataToWrite.replaceAll("\\n$", "");
					if (!fs.isFile(path)) {
						fs.createNewFile(path);
					}
					OutputStream os = fs.append(path, 100);
					BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
					br.append(finalDataToWrite + "\n");
					br.close();
					fs.close();
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<String> read(String location) {
		hdfsLoc = location;
		try{
			fs = FileSystem.get(conf);	
		}
		catch(IOException e){
			e.printStackTrace();
		}
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					Path path = new Path("hdfs://Hadoop2.synechron.com:8020/" + hdfsLoc);
					InputStream is = fs.open(path);
					BufferedReader br = new BufferedReader(new InputStreamReader(is));
					while ((stockFromFile = br.readLine()) != null) {
						String quote = stockFromFile;
						stockName = quote.trim().replaceAll("\"", "");
						stockCode.add(stockName.trim());
					}
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stockCode;
	}
}
