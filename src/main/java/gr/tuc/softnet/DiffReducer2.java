package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Double max = 0.000000000001;
		for(Text vals : value){
			double newMax = Double.parseDouble(vals.toString());
			if(newMax > max){
				max = newMax;
			}
		}
		context.write(new Text("Megisto:"), new Text(max.toString()));
	}
}
