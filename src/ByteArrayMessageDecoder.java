package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import com.linkedin.camus.coders.Message;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload into a ByteArray object,
 * System.currentTimeMillis() will be used to set CamusWrapper's
 * timestamp property

 * This MessageDecoder returns a CamusWrapper that works with ByteArray payloads,
 */
public class ByteArrayMessageDecoder extends MessageDecoder<Message, byte[]> {
  private static final Logger log = Logger.getLogger(ByteArrayMessageDecoder.class);

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<byte[]> decode(Message message) {
    //Push the raw payload and add the current time

    return new CamusWrapper<byte[]>(message, System.currentTimeMillis());
  }
}