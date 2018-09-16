package com.cts.spark_handson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.cts.util.SparkContextFactory;

import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Hello world!
 *
 */
public class App implements Serializable {
	private final String DESC = "DESC";
	private final String STATUS2 = "STATUS";
	

	public static void main(String[] args) {
		System.out.println("Hello World!");
		new App().performDq("");
	}

	private void test() {
		SparkSession spark = SparkContextFactory.getOrCreateSparkSession("local", "/user/hive/warehouse/");
		//JavaRDD<Integer> numbers = spark.sparkContext().parallelize(Arrays.asList(14,21,88,99,455));
        
        // map each line to number of words in the line
        //JavaRDD<Double> log_values = numbers.map(x -> Math.log(x)); 
        
        // collect RDD for printing
       
	}
	private void performDq(String logFile1) {
		String logFile = "/Users/raj/test.csv"; // Should be some file on your
												// system

		Map<String, List<String>> dqCheckColMap = new HashMap<>();

		List<String> nullCheckColumns = new ArrayList<>();
		nullCheckColumns.add("col1");

		List<String> typeCheckColumns = new ArrayList<>();
		typeCheckColumns.add("col1");
		typeCheckColumns.add("col2");
		typeCheckColumns.add("col6");

		dqCheckColMap.put("typeCheck", typeCheckColumns);
		dqCheckColMap.put("nullCheck", nullCheckColumns);

		SparkSession spark = SparkContextFactory.getOrCreateSparkSession("local", "/user/hive/warehouse/");
		
		
		StructType schema1 = new StructType()
			    .add("col1", "string")
			    .add("col2", "string")
			    .add("col3", "string")
			    .add("col4", "string").add("col5", "string").add("col6", "string");
		
		
		Dataset<Row> logData = spark.read().option("header", "true").schema(schema1).option("delimiter", ",").csv(logFile);
		logData.show();
		Metadata meta = new Metadata();
		StructType schema = logData.schema();
		StructField[] fields = schema.fields();
		
		Arrays.stream(fields).forEach(item -> {
			System.out.println("name: " + item.name());
			if(schema.contains(item.name())) {
				System.out.println("name: " + item.name());
			}
			else {
				
			}
		});
		
		schema.add(new StructField(STATUS2, DataTypes.IntegerType, true, meta));
		
		schema.add(new StructField(DESC, DataTypes.StringType, true, meta));
		System.out.println("Schema in JSON form: " + schema.prettyJson());
		JavaRDD<Row> javaRDD = logData.toJavaRDD();
		javaRDD.cache();
		
		System.out.println(javaRDD.count());
		List<Row> take = javaRDD.take(4);
		System.out.println(take);;
		JavaRDD<Row> processedDs = javaRDD.map(row -> {
			Seq<Object> seq = row.toSeq();
			List<Object> seqAsJavaList = new ArrayList<>(JavaConversions.seqAsJavaList(seq));
			Map<String, String> dqStats = new HashMap<>();

			dqCheckColMap.entrySet().stream().forEach(entry -> {
				int status = -1;
				String description = null;
				System.out.println("--------------------------------------------------------------");
				if (entry.getKey().equals("nullCheck")) {
					System.out.println("-----------------------------Inside NullCheck---------------------------------");
					String nullCheckStatus = entry.getValue().stream().map(column -> {
						int fieldIndex = row.fieldIndex(column);
						boolean nullAt = row.isNullAt(fieldIndex);
						System.out.println("fieldIndex: "+ fieldIndex + "  nullAt: " + nullAt);
						if (nullAt) {
							return column;
						}
						return "";
					}).collect(Collectors.joining(","));
					System.out.println("nullCheckStatus: "+ nullCheckStatus);
					if (!nullCheckStatus.replaceAll("^,+|,+$", "").isEmpty()) {
						status = 1;
						description = "Null check failed for columns: " + nullCheckStatus.replaceAll("^,+|,+$", "");
					}
				}
				System.out.println("status: "+status);
				if (status != -1) {
					dqStats.put(STATUS2,
							dqStats.containsKey(STATUS2)
									? String.valueOf(Integer.parseInt(dqStats.get(STATUS2)))
									: "1");
					dqStats.put(DESC, dqStats.containsKey(DESC)
							? String.valueOf(dqStats.get(DESC) + " and " + description) : description);
				}
			});
			System.out.println("dqStats.get(STATUS2): "+ dqStats.get(STATUS2));
			seqAsJavaList.add(dqStats.containsKey(STATUS2) ? Integer.parseInt(dqStats.get(STATUS2)) : 0);
			seqAsJavaList.add(dqStats.containsKey(DESC) ? dqStats.get(DESC) : "passed all dq checks");

			return RowFactory.create(seqAsJavaList.toArray());
		});

		processedDs.collect();
		System.out.println("COunt is: "+processedDs.count());
		System.out.println("Json: "+schema.prettyJson());
		int index = schema.fieldIndex(STATUS2);
		JavaRDD<Row> errorRecords = processedDs.filter(row -> {
			return Integer.parseInt(row.get(index).toString()) != 0;
		});

		System.out.println("----------> " + errorRecords.count());

		spark.stop();
	}
}
