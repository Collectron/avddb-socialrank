package gr.tuc.softnet;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[]  val = value.toString().split("\t");
		double varos = Double.parseDouble(val[2]);
		varos = varos*(-1);
		context.write(new DoubleWritable(varos), new Text(val[0] + "\t" + val[2]));
	}
}
