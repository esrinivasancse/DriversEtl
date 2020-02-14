package com.sky.etl.scheduler;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The Class DriversBatchProcessor.
 */
@Component
public class DriversBatchProcessor {

	/*
	 * private String sourceFilePath; private String destinationFilePath;
	 * private String fileName;
	 */

	/** The spark session. */
	SparkSession sparkSession;
	
	public DriversBatchProcessor(SparkSession sparkSession){
		this.sparkSession= sparkSession;
	}

	/**
	 * Start drivers batch processor.
	 */
	@Scheduled(cron = "${cron.value}")
	public void startDriversBatchProcessor() {
		File sourceFile = new File("./input/drivers.csv");
		if(sourceFile.exists()){
		File flag = new File("./batch.running");
		if (!flag.exists()) {
			try {
				flag.createNewFile();
				StructType schema = new StructType().add("driver", "string").add("lap_time", "double");
				Dataset<Row> driversDataset = sparkSession.read().format("com.databricks.spark.csv")
						.option("header", "false").schema(schema).load("./input/drivers.csv");
				driversDataset.show();
				driversDataset.createOrReplaceTempView("drivers_ds");
				driversDataset.printSchema();
				Dataset<Row> driversDatasetGrouped = sparkSession.sql("select driver, avg(lap_time) from drivers_ds group by driver order by avg(lap_time) asc limit 3");
				driversDatasetGrouped.show();
				driversDatasetGrouped.coalesce(1).write().mode(SaveMode.Overwrite).csv("./output/");
				flag.delete();				
				sourceFile.delete();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else {
			System.out.println("Already started batch is still running..!");
		}
		}else{
			System.out.println("These is no drivers file to Process");
		}
	}

}
