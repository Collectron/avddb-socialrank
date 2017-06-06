package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		
		double[] val = new double[2];
		int i = 0;
		for(Text vals : value){
			val[i] = Double.parseDouble(vals.toString());
			i++;
			if(i==2){break;}
		}
		Double diafora = Math.abs(val[0] - val[1]);
		context.write(key, new Text(diafora.toString()));
	}
}
