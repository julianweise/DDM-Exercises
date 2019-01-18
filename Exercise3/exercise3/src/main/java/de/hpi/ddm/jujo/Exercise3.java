package de.hpi.ddm.jujo;

import de.hpi.ddm.jujo.datatypes.AccessLog;
import de.hpi.ddm.jujo.sources.AccessLogSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

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

        File f = new File(pathToHttpLogs);
        if (!f.exists() || f.isDirectory()) {
           System.err.printf("Error: %s is not a valid log file.\n", pathToHttpLogs);
        }

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		env.addSource(new AccessLogSource(pathToHttpLogs))
				.map(new MapFunction<AccessLog, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> map(AccessLog log) {
						return new Tuple2<>(log.statusCode, 1);
					}
				})
				.keyBy(0)
				.sum(1)
				.print();

		// execute program
		env.execute("Exercise 3 Stream");
	}
}
