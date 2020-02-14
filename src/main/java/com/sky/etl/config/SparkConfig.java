package com.sky.etl.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@PropertySource("classpath:application.properties")
public class SparkConfig {
	private String appName;
	private String masterUri;
	private String sparkHome;

	public SparkConfig(@Value("${spark.app.name}") String appName, @Value("${spark.master:local}") String masterUri,
			@Value("${spark.home}") String sparkHome) {
		this.appName = appName;
		this.masterUri = masterUri;
		this.sparkHome = sparkHome;
	}

	@Bean
	public SparkConf sparkConf() {
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		SparkConf sparkConf = new SparkConf().setAppName(appName).setSparkHome(sparkHome).setMaster(masterUri);
		sparkConf.set("spark.speculation", "false");
		return sparkConf;
	}

	@Bean
	public JavaSparkContext javaSparkContext() {
		return new JavaSparkContext(sparkConf());
	}

	@Bean
	public SparkSession sparkSession() {
		return SparkSession.builder().sparkContext(javaSparkContext().sc()).appName(appName).getOrCreate();
	}

	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer();
	}
}
