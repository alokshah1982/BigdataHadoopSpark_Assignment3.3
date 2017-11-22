package mapreduce.demo.task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task4Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		//StringBuilder str = new StringBuilder();
		int tot=0;
		for (IntWritable value : values) {
			
			tot = tot + value.get();
		}

		context.write(key, new IntWritable(tot));
	}
}