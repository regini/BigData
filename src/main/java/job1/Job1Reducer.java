package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.TermFreq;

public class Job1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	private HashMap<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			if(map.containsKey(value.toString())) {
				Integer freq = map.get(value.toString());
				freq=freq+1;
				map.put(value.toString(),freq);
			} else {
				map.put(value.toString(), 1);
			}
		}

		List<TermFreq> l = new ArrayList<TermFreq>();
		for(String s : map.keySet()) {
			Text t = new Text(s);
			IntWritable f = new IntWritable(map.get(s));
			TermFreq tf = new TermFreq(t, f);
			l.add(tf);
		}
		
		Collections.sort(l);
		
		if(l.size()>10) {
			context.write(key, new Text(l.subList(0, 10).toString()));
		} else {
			context.write(key, new Text(l.toString()));
		}
	}
}

