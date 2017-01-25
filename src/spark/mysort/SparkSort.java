package spark.mysort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkSort {

	@SuppressWarnings({ "serial", "resource" })
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark Sort").setMaster(args[0]);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("dfs.replication", "1");
		long start = System.currentTimeMillis();
		JavaRDD<String> textFile = jsc.textFile(args[1]);
		JavaRDD<String> sorted = textFile.sortBy(new Function<String, String>() {
			@Override
			public String call(String line) throws Exception {
				return line.substring(0,10);
			}
		}, true, 16);
		String output = args[2];
		
		sorted.saveAsTextFile(output);
		
		long end = System.currentTimeMillis();
		long duration = (end - start)/1000;
		System.out.println("Time: " + duration + "s");
	}
		
}
