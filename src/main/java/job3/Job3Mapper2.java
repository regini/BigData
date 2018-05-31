package job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String[] parts = value.toString().split("\t");
		String couple = parts[0]; 
		String userId = parts[1]; 
		
		context.write(new Text(couple), new Text(userId));
	}
}


