package mapreduce.demo.task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		Text compnm = new Text(lineArray[0]);
		String cnm = compnm.toString();
		
		if (!cnm.equals("NA"))
		{
			context.write(compnm, new IntWritable(1));
		}
		
	}
}