package gr.tuc.softnet;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<LongWritable, LongWritable, Text, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
		Long tempKey = key.get();
		StringBuffer sBuff = new StringBuffer();
		for(LongWritable v : value){
			long temp = v.get();
			sBuff.append(temp + ";");
		}
		context.write(new Text(tempKey.toString()), new Text(sBuff.toString().substring(0, sBuff.length() - 1) + "\t1"));
	}
}
