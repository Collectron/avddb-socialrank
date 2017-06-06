package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		for(Text vals : value){
			String[] val = vals.toString().split("\t");
			context.write(new Text(val[0]), new Text(val[1]));
		}	
	}
}
