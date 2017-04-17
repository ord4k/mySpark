import java.util.Arrays;
import java.lang.Iterable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Example_4_3 {

	public static void main(String[] args) {
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf()
				.setMaster("local[10]")
				.setAppName("wordCount");
		String inputFile = "/usr/lib/spark/README.md";

		// Create a Java Spark Context.
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				return Arrays.asList(x.split(" "));
			}
		});
		System.out.println("THIS IS FIRST WORD   :" +words.take(100));
		sc.close();


	}

}
