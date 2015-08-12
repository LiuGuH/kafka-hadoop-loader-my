package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.message.MessageAndOffset;

public class TestKafkaInputFetcher {
    public static void main(String args[]) throws IOException{
    	List<String> seeds = new ArrayList<String>();
//    	seeds.add("10-140-130-101");
//    	seeds.add("10-140-130-135");
    	seeds.add("10-140-70-86");
//    	seeds.add("10-140-70-87");
    	
    	 KafkaInputFetcher fetcher = new KafkaInputFetcher("hadoop-loader", "test_hadoop", 0, seeds, 30000, 64*1024);
    
   	     long smallestOffset = fetcher.getOffset(kafka.api.OffsetRequest.EarliestTime());
         long latestOffset = fetcher.getOffset(kafka.api.OffsetRequest.LatestTime());
         fetcher.setOffset(smallestOffset);
         long readOffset=0;

        while(true){
         MessageAndOffset msg= fetcher.nextMessageAndOffset(10000,latestOffset);
         if(msg==null){
        	System.out.println("current offset is "+readOffset);
        	break;
         }


         readOffset= msg.nextOffset();

         ByteBuffer payload = msg.message().payload();

         byte[] bytes = new byte[payload.limit()];
         payload.get(bytes);
         System.out.println(String.valueOf(msg.offset()) + ": " + new String(bytes, "UTF-8"));
         

        }

        
    }
}
