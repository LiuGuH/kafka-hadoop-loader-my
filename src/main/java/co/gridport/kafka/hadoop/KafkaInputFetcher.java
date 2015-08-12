package co.gridport.kafka.hadoop;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KafkaInputFetcher {
    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private List<String> seeds;

    private String clientId;
    private String topic;
    private Integer partition;

    private String leader;
    private List<String> replicaBrokers = new ArrayList<String>();
    private SimpleConsumer consumer = null;
    private Long offset;
    private ConcurrentLinkedQueue<MessageAndOffset> messageQueue;

    public KafkaInputFetcher(String clientId, String topic, int partition, List<String> seeds, int timeout, int bufferSize) {
        this.clientId = clientId;
        this.topic = topic;
        this.partition = partition;
        this.seeds = seeds;
        messageQueue =  new ConcurrentLinkedQueue<MessageAndOffset>();
        leader = findLeader(seeds);
        consumer = new SimpleConsumer(leader, 9092, 10000, 65535, clientId);
    }
     
    
    
    public Long getOffset() {
		return offset;
	}



	public void setOffset(Long offset) {
		this.offset = offset;
	}



	/*
     * ����  messageQueue
     * ����  messageQueue�����еĵ�һ��
     * 
     */
    public MessageAndOffset nextMessageAndOffset(Integer fetchSize,long latestOffset) throws IOException {

        if (leader == null) {
            leader = findLeader(seeds);       
        }
        if (consumer == null) {
        	 consumer = new SimpleConsumer(leader, 9092, 10000, 65535, clientId);
        }

           if (messageQueue.size() ==0 && offset < latestOffset) {
            log.info("{} fetching offset {} ", topic+":" + partition, offset);
            FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
            FetchRequest req = requestBuilder
                .clientId(clientId)
                .addFetch(topic, partition, offset, fetchSize)
                .build();
            
           FetchResponse response = consumer.fetch(req);
            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leader + " Reason: " + code);
                close();
                leader = findNewLeader(leader);
            }

            for (MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
       
                //Also note that we are explicitly checking that the offset being read is not less than the offset that we requested. This is needed since if Kafka is compressing the messages, the fetch request will return an entire compressed block even if the requested offset isn't the beginning of the compressed block. 
                if (currentOffset < offset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }
                System.out.println(messageAndOffset+"------");
                messageQueue.offer(messageAndOffset);
            }

        }
        if (messageQueue.size() > 0) {
        	//�õ�������ĵ�һ��MessageAndOffset
            MessageAndOffset messageAndOffset = messageQueue.poll();
            //����offset��һ�¸���Ϣ
            offset = messageAndOffset.nextOffset();
            //������ʼ��offest
            return messageAndOffset;
        } else {
            return null;
        }

    }
    /*
     * �õ�partition��offset
     * Finding Starting Offset for Reads
     */
    public Long getOffset(Long time) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
       
       if (leader == null) {
           leader = findLeader(seeds);
       }
       if (consumer == null) {
           consumer = new SimpleConsumer(leader, 9092, 10000, 65535,  clientId);
       }
        OffsetResponse response1 = consumer.getOffsetsBefore(request);

        if (response1.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response1.errorCode(topic, partition) );
            System.exit(1);
        }
        return response1.offsets(topic, partition)[0];
    }
    

    private String findNewLeader(String oldLeader) throws IOException {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            String newLeader= findLeader(replicaBrokers);
            if (newLeader == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(newLeader) && i == 0) {
                goToSleep = true;
            } else {
                return newLeader;
            }
            if (goToSleep) {
                try { Thread.sleep(1000); } catch (InterruptedException ie) {}
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new IOException("Unable to find new leader after Broker failure. Exiting");
    }

    private String findLeader(List<String> seeds) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seeds) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, 9092, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null && returnMetaData.leader() != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
            return returnMetaData.leader().host();
        } else {
            return null;
        }
    }

    public void close() {
       if (consumer != null) {
           consumer.close();
           consumer = null;
       }
    }
    
    

}
