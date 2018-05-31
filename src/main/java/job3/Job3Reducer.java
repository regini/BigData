package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends Reducer<Text, Text, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashSet<String> set = new HashSet<String>();
		
		for(Text t : values) {
			set.add(t.toString());
		}
		
		ArrayList<String> list = new ArrayList<String>(set);
		
		for(int i=0; i<list.size()-1; i++) {
			for(int j=i+1; j<list.size(); j++) {
				context.write(new Text("[" + list.get(i) + " , " + list.get(j) + "]"), key);
			}
		}
		
	}
}