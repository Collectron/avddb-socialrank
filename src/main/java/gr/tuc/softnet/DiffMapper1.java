package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] val = value.toString().split("\\t");
		context.write(new Text(val[0]), new Text(val[2]));
	}
}
