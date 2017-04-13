
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.io.Serializable;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class AvgCount implements Serializable {

	private static final long serialVersionUID = -4138060241687412320L;

	public int total_;
	public int num_;

	public AvgCount(int total, int num) {
		total_ = total;
		num_ = num;
	}

	public float avg() {
		return total_ / (float) num_;
	}

	
			

		


	public static void main(String[] args) {
		
		Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
			public AvgCount call(Integer x) {
				return new AvgCount(x,1);
			}
		};
			
		Function2<AvgCount, Integer, AvgCount> addAndCount =
					new Function2<AvgCount, Integer, AvgCount>() {
						public AvgCount call(AvgCount a, Integer x) {
							a.total_ += x;
							a.num_ += 1;
							return a;
				}
			};
				
		Function2<AvgCount, AvgCount, AvgCount> combine = 
				new Function2<AvgCount, AvgCount, AvgCount>() {
					public AvgCount call(AvgCount a, AvgCount b) {
						a.total_ += b.total_;
						a.num_ += b.num_;
						return a;
					}
				};
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("wordCount");
		String inputFile = "/home/ord4k/Documents/data";

		// Create a Java Spark Context.
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
/*		Map<String,Integer> sm = new TreeMap<String,Integer>();
		sm.put("dom", 1);
		sm.put("dom", 1);
		sm.put("dom", 1);
		sm.put("as", 1);*/
		JavaRDD<String> input = sc.textFile(inputFile);
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				return Arrays.asList(x.split(" "));
			}
		});
		JavaPairRDD<String, Integer> result = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String x) { return new Tuple2(x, 1); }
	
				//reduceByKey VS combineByKey: the commented code is from previous example//	
					/*				}).reduceByKey(
						new Function2<Integer, Integer, Integer>() {
								public Integer call(Integer a, Integer b) { return a + b; }*/
				});
	
		AvgCount initial = new AvgCount(0,0);
		JavaPairRDD<String, AvgCount> avgCounts = result.combineByKey(createAcc, addAndCount, combine);
		Map<String, AvgCount> countMap = avgCounts.collectAsMap();
		
		for (Entry<String, AvgCount> entry : countMap.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue().avg());
		}
	
	}

}
