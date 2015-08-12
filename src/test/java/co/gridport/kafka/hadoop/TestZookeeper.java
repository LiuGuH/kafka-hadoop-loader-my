package co.gridport.kafka.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

import co.gridport.kafka.hadoop.ZkUtils.StringSerializer;

public class TestZookeeper {
	
	static String BROKER_TOPICS_PATH ="/brokers/topics";
	static String topic="test-10m";
    static String s = "/brokers/topics/test-10m/partitions";
    public static void main(String args[]){
    	ZkClient client = new ZkClient("sdf-spark-master1:31818,sdf-spark-master2:31818,sdf-resourcemanager1:31818,sdf-resourcemanager2:31818,sdf-logserver:31818/test-kafka",10000,10000,new StringSerializer());
    
    	
    
         
         List<String> partitions = new ArrayList<String>();
         List<String> brokersTopics = client.getChildren( BROKER_TOPICS_PATH + "/" + topic);
         
//         List<String> brokersTopics = client.getChildren(s);
    
//         List<String> children = client.getChildren(path);
         
         
         String test =  client.readData("/brokers");
         System.out.println(test);
         
         
/*         for(String broker: brokersTopics) {
        	 System.out.println(broker);
             String parts = client.readData(BROKER_TOPICS_PATH + "/" + topic + "/" +broker);
             for(int i =0; i< Integer.valueOf(parts); i++) {
            	 System.out.println(broker);
                 partitions.add(broker + "-" + i);
             }
         }*/
       
    }
}
