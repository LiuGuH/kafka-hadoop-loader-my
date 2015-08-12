package co.gridport.kafka.hadoop;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtils implements Closeable {

    private static Logger log = LoggerFactory.getLogger(ZkUtils.class);

    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";

    private ZkClient client ;
    Map<String, String> brokers ;
    List<String> seeds = new ArrayList<String>();

    public ZkUtils(String zkConnectString, int sessionTimeout, int connectTimeout) {
        client = new ZkClient(zkConnectString, sessionTimeout, connectTimeout, new StringSerializer() );
        log.info("Connected zk");
    }

    public ZkUtils(String zkConnectString)
    {
        this(zkConnectString, 10000, 10000);
    }
    

    /*
     * 得到broker 的 ip列表
     */
    public List<String> getSeedList() {

        if (seeds == null) {
        seeds = new ArrayList<String>();
        }
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
            for(String bid: brokerIds) {
                String data = client.readData(BROKER_IDS_PATH + "/" + bid);
                log.info("Broker " + bid + " " + data);
                String  host = data.split(",")[2].split(":")[1].split("\"")[1];  
                brokers.put(bid, host);
                seeds.add(brokers.get(bid));
            }
        
        return seeds;
    }


    private String getOffsetsPath(String group, String topic, Integer partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }
   /*
    * group 消费者组
    * topic 
    * partition
    * 得到该group的partition的当前消费到的offset
    */
    public long getLastConsumedOffset(String group, String topic, Integer partition) {
        String znode = getOffsetsPath(group ,topic ,partition);
        String offset = client.readData(znode, true);
        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }
     /*
      * client提交partition重置的offset
      */
    public void commitLastConsumedOffset(
        String group, 
        String topic, 
        Integer partition, 
        long offset
    )
    {
        String path = getOffsetsPath(group ,topic ,partition);

        log.info("OFFSET COMMIT " + path + " = " + offset);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        //TODO JIRA:EDA-4 use versioned zk.writeData in case antoher hadooop loaer has advanced the offset
        client.writeData(path, offset);
    }

    /*
     * 得到zookeeper当前path的下一级的znode目录
     */
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }

    public void close() throws java.io.IOException {
        if (client != null) {
            client.close();
        }
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {}

        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
    }
    

}
