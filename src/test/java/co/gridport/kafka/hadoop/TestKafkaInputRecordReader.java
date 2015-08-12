package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kafka.message.MessageAndOffset;

public class TestKafkaInputRecordReader {
    public static void main(String args[]) throws IOException, InterruptedException{
    	List<String> seeds = new ArrayList<String>();
    	seeds.add("10-140-130-101");
    	seeds.add("10-140-130-135");
    	seeds.add("10-140-70-86");
    	seeds.add("10-140-70-87");
    	
    	 KafkaInputFetcher fetcher = new KafkaInputFetcher("hadoop-loader", "test_single", 0, seeds, 30000, 64*1024);
    	 System.out.println(fetcher);
   	     long smallestOffset = fetcher.getOffset(kafka.api.OffsetRequest.EarliestTime());
         System.out.println("smallestOffset====="+smallestOffset);
         long latestOffset = fetcher.getOffset(kafka.api.OffsetRequest.LatestTime());
         System.out.println("latestOffset====="+latestOffset);
         
         MessageAndOffset msg= fetcher.nextMessageAndOffset(100,latestOffset);

         System.out.println(msg);
                
    }
}
