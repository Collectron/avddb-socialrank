package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InitMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String k = value.toString();
		String[] temp = k.split("\t");
		context.write(new LongWritable(Long.parseLong(temp[0])), new LongWritable(Long.parseLong(temp[1])));
	}
}
