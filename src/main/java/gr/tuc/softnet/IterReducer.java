package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		String edges = "";
		Double sum = 0.15;
		
		for(Text val : value){
			String temp = val.toString();
			if(temp.contains("\t")){
				String[] vars = temp.split("\t");
				edges = vars[0];
			}
			else{
				if(!temp.isEmpty())
					sum = sum + 0.85 * Double.parseDouble(temp); //Sum contributions from other nodes
			}
		}
		context.write(key, new Text(edges + "\t" + sum.toString()));

	}
}
