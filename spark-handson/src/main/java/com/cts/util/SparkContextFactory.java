package com.cts.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkContextFactory {
	public static Logger logger = LoggerFactory.getLogger(SparkContextFactory.class);
	
	public static JavaSparkContext sparkInit(String sparkMaster) {
		logger.info("Creating Spark Configuration");
		// Create an Object of Spark Configuration
		SparkConf javaConf = new SparkConf();
		javaConf.set("spark.driver.allowMultipleContexts", "true");
		javaConf.setAppName("NPP Data Ingestion");
		javaConf.setMaster(sparkMaster);
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(javaConf));
		logger.info("Creating Spark Context");
		return ctx;
	}
	
	
	public static SparkSession getOrCreateSparkSession(String sparkMaster, String hiveWareHouseDir){
		SparkSession sparkSession = SparkSession
				  .builder()
				  .appName("NPP Data Ingestion").master(sparkMaster).config("hive.metastore.warehouse.dir", hiveWareHouseDir)
				  .config("spark.sql.warehouse.dir", hiveWareHouseDir)
				  .enableHiveSupport()
				  .getOrCreate();
		return sparkSession;
	}
}
