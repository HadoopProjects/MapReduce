import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException{
		
		String record = inputValue.toString();
		String[] splits = record.split("#");
		
		int year = Integer.parseInt(splits[0]);
		int value = Integer.parseInt(splits[1]);
		
		context.write(new IntWritable(year), new IntWritable(value));
	}

}
