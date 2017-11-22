package mapreduce.demo.task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		Text compnm = new Text(lineArray[0]);
		Text txtstate = new Text(lineArray[3]);
		String cnm = compnm.toString();
		String state = txtstate.toString();
		if (cnm.equals("Onida"))
		{
			context.write(txtstate, new IntWritable(1));
		}
		
	}
}