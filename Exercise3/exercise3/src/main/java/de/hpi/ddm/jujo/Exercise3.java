package de.hpi.ddm.jujo;

import de.hpi.ddm.jujo.datatypes.AccessLog;
import de.hpi.ddm.jujo.sources.AccessLogSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.addSource(new AccessLogSource(pathToHttpLogs))
				.map((MapFunction<AccessLog, Tuple2<Integer, Integer>>) log -> new Tuple2<>(log.statusCode, 1))
				.returns(Types.TUPLE(Types.INT, Types.INT))
				.keyBy(0)
				.flatMap(new Accumulator());



		// execute program
		JobExecutionResult result = env.execute("Exercise 3 Stream");
		result.getAccumulatorResult("statusCounter");
		result.getAllAccumulatorResults();
	}

	public static class Accumulator extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {


		@Override
		public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			getRuntimeContext().addAccumulator();
			// statusCounter.add(input.f1);
		}
	}
}
