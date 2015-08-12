package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class TestKafkaInputFormat {
	  public static void main(String args[]) throws IOException{
    	  ZkUtils zk = new ZkUtils(
    	           "sdf-spark-master1:31818,sdf-spark-master2:31818,sdf-resourcemanager1:31818,sdf-resourcemanager2:31818,sdf-logserver:31818/test-kafka",
    	          10000,
    	           10000
    	        );
    	        List<String> seeds = zk.getSeedList();

    	        String consumerGroup = "test_hadoop111";

    	        List<String> topics = Arrays.asList("test_single");
    	        
    	        for(final String seed: seeds) {
    	            SimpleConsumer consumer = new SimpleConsumer(seed, 9092, 10000, 65535, "PartitionsLookup");
    	            TopicMetadataRequest request = new TopicMetadataRequest(topics);
    	            TopicMetadataResponse response = consumer.send(request);
    	            if (response != null && response.topicsMetadata() != null) {
                       for(TopicMetadata tm: response.topicsMetadata()) {
    	                    for(PartitionMetadata pm: tm.partitionsMetadata()) {
    	                        long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, tm.topic(), pm.partitionId()) ;
    	                        System.out.println(lastConsumedOffset);
    	                        System.out.println(seed+"-----"+tm.topic()+"------"+pm.partitionId()+"------"+lastConsumedOffset);
    	                        InputSplit split = new KafkaInputSplit(
    	                        		seed,
    	                            tm.topic(), 
    	                            pm.partitionId(), 
    	                            lastConsumedOffset
    	                        );
    	                        
    	               
    	                    }
    	                }
    	            }
    	            break;
    	        }
    	        zk.close();
    }
}
