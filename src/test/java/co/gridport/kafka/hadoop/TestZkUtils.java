package co.gridport.kafka.hadoop;

import java.util.List;

public class TestZkUtils {
    
    public static void main(String args[]){
    	ZkUtils zkutils = new ZkUtils("sdf-spark-master1:31818,sdf-spark-master2:31818,sdf-resourcemanager1:31818,sdf-resourcemanager2:31818,sdf-logserver:31818/test-kafka");
    	List<String> list = zkutils.getSeedList();
    	for(String s :list)
    		System.out.println(s);
    	
    	 //µÃµ½partitionµÄid	 
//    	long s= zkutils.getLastConsumedOffset("consumer-test","test",0);
//    	System.out.println(s);
//    	
//    	zkutils.commitLastConsumedOffset("consumer-test", "test", 0, 10);
    	
    }

}
