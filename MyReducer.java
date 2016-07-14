import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<IntWritable,IntWritable, IntWritable, LongWritable> {
	
	public void reduce(IntWritable inputKey,Iterable<IntWritable> inputValues, Context context) throws IOException, InterruptedException{
		
		long max = 0;
		
		for(IntWritable input : inputValues){
			if(max < input.get()){
				max = input.get();
			}
		}
		context.write(inputKey, new LongWritable(max));
		
	}

}
