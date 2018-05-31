package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.ScoreByYear;

public class Job2 {
	
	public static void main(String[] args) throws Exception {
		double startTime = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Errore");
			System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Job 2");
		job.setJarByClass(Job2.class);
		job.setMapperClass(Job2Mapper.class);
		job.setReducerClass(Job2Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreByYear.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		
		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		System.out.println("Tempo di esecuzione Job2 MR:\t" + elapsedTime + "s");
	}
}
