package com.sky.etl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.sky.etl.config.SparkConfig;
import com.sky.etl.scheduler.DriversBatchProcessor;

public class DriversBatchProcessorTest {

	private DriversBatchProcessor driversBatchProcessor;
	private SparkConfig sparkConfig;
	private static String SPARK_APP_NAME = "DriversEtl Spark Application";
	private static String SPARK_MASTER_URI = "local[*]";
	private static String SPARK_SPARK_HOME = "src/main/resources";

	@Before()
	public void setup() throws IOException {
		sparkConfig = new SparkConfig(SPARK_APP_NAME, SPARK_MASTER_URI, SPARK_SPARK_HOME);
		driversBatchProcessor = new DriversBatchProcessor(sparkConfig.sparkSession());
		new File("./batch.running").deleteOnExit();
		new File("./input/drivers.csv").deleteOnExit();
		FileUtils.copyFile(new File("drivers.csv"), new File("./input/drivers.csv"));
	}

	@Test
	public void checkCsvParsing() throws IOException {
		driversBatchProcessor.startDriversBatchProcessor();
		List<List<String>> csvResult = null;
		Path dirPath = Paths.get("./output/");
		Stream<Path> pathStream = Files.find(dirPath, 100, (path, basicFileAttributes) -> {
			File file = path.toFile();
			return !file.isDirectory() && file.getName().contains("part-") && file.getName().endsWith(".csv");
		});
		try (Stream<String> lines = Files.lines(pathStream.findFirst().get())) {
			csvResult = lines.map(line -> Arrays.asList(line.split(","))).collect(Collectors.toList());
			// csvResult.forEach(value -> System.out.print(value));
		} catch (IOException e) {
			e.printStackTrace();
		}
		assertEquals(csvResult.size(), 3);
		assertEquals(csvResult.get(0).get(0), "Alonzo");
		assertEquals(csvResult.get(0).get(1), "4.526666666666666");
		assertEquals(csvResult.get(1).get(0), "Hamilton");
		assertEquals(csvResult.get(1).get(1), "4.5633333333333335");
		assertEquals(csvResult.get(2).get(0), "Verstrappen");
		assertEquals(csvResult.get(2).get(1), "4.63");
	}

}
