package de.hpi.ddm.jujo;

import de.hpi.ddm.jujo.datatypes.AccessLog;
import de.hpi.ddm.jujo.sources.AccessLogSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Exercise3 {

	private static final int DEFAULT_NUMBER_OF_CORES = 4;
	private static final String DEFAULT_PATH_TO_HTTP_LOGS = "./access_log_Aug95";

	private static final String STATUS_CODE_ACCUMULATOR_PREFIX = "statusCodeAccumulator";
	private static final String INVALID_LOG_ACCUMULATOR = "invalidLogAccumulator";

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

        OutputTag<String> invalidLogTag = new OutputTag<String>("invalidLogTag"){};

        File f = new File(pathToHttpLogs);
        if (!f.exists() || f.isDirectory()) {
           System.err.printf("Error: %s is not a valid log file.\n", pathToHttpLogs);
        }

		StreamExecutionEnvironment.setDefaultLocalParallelism(numberOfCores);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<AccessLog> accessLogDataStream = env.readTextFile(pathToHttpLogs)
                .map(AccessLog::fromString)
                .process(new ProcessFunction<AccessLog, AccessLog>() {

                    @Override
                    public void processElement(
                            AccessLog value,
                            Context ctx,
                            Collector<AccessLog> out) {

                        if (value.isValid) {
                            out.collect(value);
                            return;
                        }
                        ctx.output(invalidLogTag, value.rawLine);
                    }
                });

        accessLogDataStream.map((MapFunction<AccessLog, Tuple2<Integer, Integer>>) log -> new Tuple2<>(log.statusCode, 1))
				.returns(Types.TUPLE(Types.INT, Types.INT))
				.keyBy(0)
				.flatMap(new Accumulator());

        DataStream<String> sideOutputStream = accessLogDataStream.getSideOutput(invalidLogTag)
                .map(new InvalidLineAccumulator());


        // execute program
		JobExecutionResult result = env.execute("Exercise 3 Stream");
		Map<String, Object> resultSet = result.getAllAccumulatorResults();

		double validLogs = printStatusCodes(resultSet);
		int invalidLogs = result.getAccumulatorResult(INVALID_LOG_ACCUMULATOR);

		System.out.println("Invalid log lines: " + formatDouble(invalidLogs / (invalidLogs + validLogs) * 100) + " %");
	}

	private static double printStatusCodes(Map<String, Object> resultSet) {
		double totalRequests = 0;
        Collection<String> sortedStatusCodes = resultSet.keySet().stream()
                        .filter(key -> key.startsWith(STATUS_CODE_ACCUMULATOR_PREFIX))
                        .sorted()
                        .collect(Collectors.toCollection(ArrayList::new));

		StringBuilder resultOutputBuilder = new StringBuilder();
		resultOutputBuilder.append("Status code occurrences: \n");

		for (Object occurrence : resultSet.values()) {
			totalRequests += (int) occurrence;
		}

		for (String statusCode : sortedStatusCodes) {
			resultOutputBuilder.append("\t • ");
			resultOutputBuilder.append(statusCode.replace(STATUS_CODE_ACCUMULATOR_PREFIX, ""));
			resultOutputBuilder.append(": ");
			resultOutputBuilder.append(formatDouble((int) resultSet.get(statusCode) / totalRequests * 100));
			resultOutputBuilder.append(" %\n");
		}

		System.out.print(resultOutputBuilder.toString());

		return totalRequests;
	}

	private static String formatDouble(double input) {
        DecimalFormat decimalFormatter = new DecimalFormat("0.0000");
        return decimalFormatter.format(input);
    }

	public static class Accumulator extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		ValueState<Boolean> statusAccumulatorExists;

		@Override
		public void open(Configuration config) {
			statusAccumulatorExists = getRuntimeContext().getState(new ValueStateDescriptor<>("statusAccumulatorExists", boolean.class));
		}

		@Override
		public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			if (statusAccumulatorExists.value() == null || !statusAccumulatorExists.value()) {
				getRuntimeContext().addAccumulator(getAccumulatorName(input.f0), new IntCounter());
				statusAccumulatorExists.update(true);
			}

			getRuntimeContext().getAccumulator(getAccumulatorName(input.f0)).add(input.f1);
		}

		private String getAccumulatorName(int statusCode) {
		    return STATUS_CODE_ACCUMULATOR_PREFIX + statusCode;
        }
	}

	public static class InvalidLineAccumulator extends RichMapFunction<String, String> {

        @Override
        public void open(Configuration config) {
            getRuntimeContext().addAccumulator(INVALID_LOG_ACCUMULATOR, new IntCounter());
        }

        @Override
        public String map(String value) throws Exception {
            getRuntimeContext().getAccumulator(INVALID_LOG_ACCUMULATOR).add(1);
            return value;
        }
    }
}
