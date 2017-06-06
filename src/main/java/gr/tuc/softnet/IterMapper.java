package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\t");
		String src = vals[0];
		String[] edges = vals[1].split(";"); 
		int length = edges.length;
		Double weight = Double.parseDouble(vals[2]); 
		Double cont = weight/length; 

		for(String edgs : edges){
			context.write(new Text(edgs), new Text(cont.toString()));
		}
		context.write(new Text(src), new Text(vals[1] + "\t" + vals[2]));
		
	}
}
