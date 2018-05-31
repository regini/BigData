package job3;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer2 extends Reducer<Text, Text, Text, IntWritable> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashSet<Text> set = new HashSet<Text>();
		for(Text t : values) {
			set.add(t);
		}
		context.write(key,  new IntWritable(set.size()));
	}
}