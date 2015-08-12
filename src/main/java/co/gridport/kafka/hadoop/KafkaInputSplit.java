package co.gridport.kafka.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class KafkaInputSplit extends InputSplit implements Writable {
    
    private String broker;
    private int partition;
    private String topic;
    private long lastCommit;

    public KafkaInputSplit() {}

    public KafkaInputSplit(String broker, String topic, int partition, long lastCommit) {
        this.broker = broker;  
        this.partition = partition;
        this.topic = topic;
        this.lastCommit = lastCommit;
    }
   
    public void readFields(DataInput in) throws IOException {
        broker = Text.readString(in);
        topic = Text.readString(in);
        partition = in.readInt();
        lastCommit = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, broker);
        Text.writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(lastCommit);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
       return new String[] {broker};
    }
    
    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getWatermark() {
        return lastCommit;
    }

    @Override
    public String toString() {
        return   "-" + topic + "-" + partition + "-" + lastCommit ;
    }
}