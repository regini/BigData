package job1;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.opencsv.RFC4180Parser;

import scala.Tuple2;

import utils.AmazonFineFoodReviews;
import utils.UnixTimeConverter;

public class Job1Spark implements Serializable{

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;

	public Job1Spark(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {
		double startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Job1Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);;

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job1Spark bpm = new Job1Spark(args[0], args[1]);
		bpm.run(sc);

		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		sc.close();
		sc.stop();
		System.out.println("Tempo di esecuzione Job1 Spark:\t" + elapsedTime + "s");
	}

	private void run(JavaSparkContext sc) {
		JavaPairRDD<Integer, Iterable<List<String>>> couple = loadData(sc).groupByKey();

		JavaPairRDD<Integer, LinkedHashMap<String, Integer>> result = couple.mapToPair( x -> {
			LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
			Iterable<List<String>> it = x._2;
			for(List<String> l : it) {
				for(String s : l) {
					if (map.containsKey(s)) {
						Integer freq = map.get(s);
						freq=freq+1;
						map.put(s, freq);
					} else {
						map.put(s, 1);
					}
				}
			}

			LinkedHashMap<String, Integer> sortedMap = (LinkedHashMap<String, Integer>) sortByValues(map);
			if(sortedMap.size()>10) {
				LinkedHashMap<String, Integer> sortedMapSeized = sortedMap.entrySet().stream()
						.limit(10)
						.collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
				return new Tuple2<Integer,LinkedHashMap<String, Integer>>(x._1(), sortedMapSeized);
			} else {
				return new Tuple2<Integer,LinkedHashMap<String, Integer>>(x._1(), sortedMap);
			}
		});

		result.groupByKey().sortByKey().saveAsTextFile(outputFolderPath);

	}

	public JavaPairRDD<Integer, List<String>> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputFilePath);

		JavaPairRDD<Integer, List<String>> couple = 
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
					
					ArrayList<String> list = new ArrayList<>();
					StringTokenizer tokenizer = new StringTokenizer(values[AmazonFineFoodReviews.Summary].toLowerCase());
					while (tokenizer.hasMoreTokens()) {
						String word = tokenizer.nextToken();
						list.add(word);
					}	
					return new Tuple2<Integer, List<String>>(UnixTimeConverter.time2Year(Long.parseLong(values[AmazonFineFoodReviews.Time])), 
							list);
				});

		return couple;
	}

	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {


			@SuppressWarnings("unchecked")
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		//which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
}


