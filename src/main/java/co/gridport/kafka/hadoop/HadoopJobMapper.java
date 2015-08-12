package co.gridport.kafka.hadoop;

import java.io.IOException;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopJobMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {

    @Override
    public void map(LongWritable key, BytesWritable value, Context context) throws IOException {

        	 try {
				context.write(new Text(String.valueOf(key)), new Text(value.copyBytes()));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
 

    }

   
}
