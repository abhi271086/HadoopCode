import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Verb;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class GoogleFinanceConsumer extends Thread {
	
	
	private static final String STREAM_URI = "http://www.google.com/finance/getprices?q=AAPL";

    public void run(){
        try{
          String Screen_Name="googleStockData";
   		  while(true)
   		  {
            System.out.println("Starting finance Stream");
            Properties props = new Properties();
    	    props.put("metadata.broker.list", "Hadoop2.synechron.com:9092");
    	    props.put("serializer.class", "kafka.serializer.StringEncoder");
    	    props.put("request.required.acks", "1");
    	    props.put("retry.backoff.ms", "150");
    	    props.put("message.send.max.retries","10");
    	    props.put("topic.metadata.refresh.interval.ms","0");
    	    props.put("client.id","camus");

    	    @SuppressWarnings("deprecation")
			ProducerConfig config = new ProducerConfig(props);

    	    @SuppressWarnings("deprecation")
			final Producer<String, String> producer = new Producer<String, String>(config);
            // Let's generate the request
            OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
            request.addHeader("version", "HTTP/1.1");
            request.addHeader("host", "http://www.google.com/finance");
            request.setConnectionKeepAlive(true);
            request.addHeader("user-agent", "Google Finance Stream Reader");
            request.addBodyParameter("track", Screen_Name); // Set keywords you'd like to track here
            Response response = request.send();
   		  	
            // Create a reader to read Twitter's stream
           BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));
   		  	
           String line;
       while ((line = reader.readLine()) != null) {
    	   KeyedMessage<String, String> data = new KeyedMessage<String, String>("googleStockData",line);
                System.out.println(line);
                producer.send(data);
                Thread.sleep(10000);
            }
   		  //}
        }
        }     
        catch (IOException ioe){
            System.out.println(ioe.getMessage());
        	ioe.printStackTrace();
        
        } catch (InterruptedException e) {
			// TODO Auto-generated catch block
        	System.out.println(e.getMessage());
        	e.printStackTrace();
		}

    }

public static void main(String[] args){

    GoogleFinanceConsumer streamConsumer = new GoogleFinanceConsumer();
    streamConsumer.start();

}
}
    