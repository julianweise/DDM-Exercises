package de.hpi.ddm.jujo;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Exercise3 {

	private static final int DEFAULT_NUMBER_OF_CORES = 4;
	private static final String DEFAULT_PATH_TO_HTTP_LOGS = "./access_log_Aug95";

	private static int numberOfCores = DEFAULT_NUMBER_OF_CORES;
	private static String pathToHttpLogs = DEFAULT_PATH_TO_HTTP_LOGS;

	public static void main(String[] args) throws Exception {

		final ParameterTool params;
		try {
			params = ParameterTool.fromArgs(args);
		} catch (IllegalArgumentException e) {
			System.err.printf("Error while parsing command line arguments: %s \n", e.getMessage());
			return;
		}

		numberOfCores = params.getInt("cores", DEFAULT_NUMBER_OF_CORES);
		pathToHttpLogs = params.get("path", DEFAULT_PATH_TO_HTTP_LOGS);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
