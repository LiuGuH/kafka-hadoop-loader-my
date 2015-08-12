package co.gridport.kafka.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestMapper {
	 
    public static void main(String args[]) {
    	HadoopJobMapper maptest = new HadoopJobMapper();
    	LongWritable lo =  new LongWritable();
    	lo.set(0l);
    	BytesWritable  value = new BytesWritable();
        String s = "1438913587232,www.example.com,192.168.2.638";
      //  s="{key1:value1}";
        value.set(s.getBytes(),0,s.length());
        System.out.println(new String(s.getBytes()));
        System.out.println(s.getBytes().length);
        Text text = new Text(value.copyBytes());
      
        
         System.out.println(text.getBytes().length);	
    }
}
