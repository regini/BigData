package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MathsUtils;
import utils.ScoreByYear;

public class Job2Reducer extends Reducer<Text, ScoreByYear, Text, Text> {
	public static final int StartYear = 2003;
	public static final int FinalYear = 2012;
	
	@Override
	public void reduce(Text key, Iterable<ScoreByYear> values, Context context) throws IOException, InterruptedException {
		HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		ArrayList<ScoreByYear> score = new ArrayList<ScoreByYear>();

		for (ScoreByYear s : values) {
			ScoreByYear tmp = new ScoreByYear(new IntWritable(s.getScore()), new IntWritable(s.getYear()));
			score.add(tmp);
		}

		for(int i = StartYear; i<=FinalYear; i++) {
			int num=0;
			int somma=0;
			double media=0;
			for (ScoreByYear s : score) {
				if (s.getYear()==i) {
					somma = somma + s.getScore();
					num++;
				}
			}
			if(num!=0) {
				media = MathsUtils.round((double)somma/(double)num,2);
			}
			map.put(i, media);
		}
		context.write(key, new Text(map.toString()));
	}
}