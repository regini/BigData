package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3 {
	
	public static void main(String[] args) throws Exception {
		double startTime = System.currentTimeMillis();
		
		Job job1 = new Job(new Configuration(), "Job 3A");
		job1.setJarByClass(Job3.class);
		job1.setMapperClass(Job3Mapper.class);
		job1.setReducerClass(Job3Reducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(new Configuration(),"Job 3B");
		job2.setJarByClass(Job3.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Job3Mapper2.class);
		job2.setReducerClass(Job3Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);
		
		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		System.out.println("Tempo di esecuzione Job3 MR:\t" + elapsedTime + "s");
	}
}
