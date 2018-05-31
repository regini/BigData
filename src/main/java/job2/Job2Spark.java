package job2;

import java.io.Serializable;
import java.util.LinkedHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.opencsv.RFC4180Parser;

import scala.Tuple2;

import utils.AmazonFineFoodReviews;
import utils.MathsUtils;
import utils.UnixTimeConverter;

public class Job2Spark implements Serializable{

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;

	private static final int StartYear = 2003;
	private static final int FinalYear = 2012;

	public Job2Spark(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {
		double startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Job2Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);;

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job2Spark bpm = new Job2Spark(args[0], args[1]);
		bpm.run(sc);

		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		sc.close();
		sc.stop();
		System.out.println("Tempo di esecuzione Job2 Spark:\t" + elapsedTime + "s");
	}

	private void run(JavaSparkContext sc) {
		JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> data = loadData(sc).groupByKey();

		JavaPairRDD<String, LinkedHashMap<Integer, Double>> result = data.mapToPair( x -> {
			LinkedHashMap<Integer, Double> map = new LinkedHashMap<Integer, Double>();

			Iterable<Tuple2<Integer, Integer>> iterable = x._2();

			for(int i = StartYear; i<=FinalYear; i++) {
				int num=0;
				int somma=0;
				double media=0;
				for (Tuple2<Integer, Integer> t : iterable) {
					if (t._1()==i) {
						somma = somma + t._2();
						num++;
					}
				}
				if(num!=0) {
					media = MathsUtils.round((double)somma/(double)num,2);
				}
				map.put(new Integer(i), new Double(media));
			}

			return new Tuple2<String,LinkedHashMap<Integer, Double>>(x._1(), map);
		}); 
		result.sortByKey().saveAsTextFile(outputFolderPath);
	}

	public JavaPairRDD<String, Tuple2<Integer, Integer>> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputFilePath);

		JavaPairRDD<String, Tuple2<Integer, Integer>> data = 
				words.mapPartitionsWithIndex((index, iter) -> {
					if (index == 0 && iter.hasNext()) {
						iter.next();
						if (iter.hasNext()) {
							iter.next();
						}
					}
					return iter;
				}, true)
				.mapToPair(row -> {
					RFC4180Parser parser = new RFC4180Parser();
					String[] values = parser.parseLine(row);

					return new Tuple2<String, Tuple2<Integer, Integer>>(values[AmazonFineFoodReviews.ProductId],
							new Tuple2<Integer, Integer>(UnixTimeConverter.time2Year(Long.parseLong(values[AmazonFineFoodReviews.Time])), 
									Integer.parseInt(values[AmazonFineFoodReviews.Score])));
				});

		return data;
	}
}



