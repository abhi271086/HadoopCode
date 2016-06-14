import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

public class HdfsTest {

    	public static void write( String abc) {
    		final String line = abc;
        try {
            UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("root");
            
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
            	
                public Void run() throws Exception {
                	
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://Hadoop2.synechron.com:8020/user/hduser");
                    conf.set("hadoop.job.ugi", "root");

                    Path path = new Path("hdfs://Hadoop2.synechron.com:8020/user/hduser/HDFSWritetest");
                    
                    FileSystem fs = FileSystem.get(conf);
                    if(!fs.isFile(path)){
                    	fs.createNewFile(new Path("/user/hduser/HDFSWritetest"));	
                    };
                    
                    
                    
                    OutputStream os = fs.append( path, 100,
                    	    new Progressable() {
                    	        public void progress() {
                    	            System.out.println("...bytes written");
                    	        } });
                    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                    	br.append(line + '\n');
                    	br.close();
                    	fs.close();
/*
                    
                    FileStatus[] status = fs.listStatus(new Path("/user/hduser"));
                    for(int i=0;i<status.length;i++){
                        System.out.println(status[i].getPath());
                    }*/
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
