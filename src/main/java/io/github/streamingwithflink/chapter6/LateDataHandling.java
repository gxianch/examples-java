package io.github.streamingwithflink.chapter6;

import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;

/**
 * 改造自example-scala分支
 * 
 * @author guoxian
 * @date 2021/03/05
 */
public class LateDataHandling {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// checkpoint every 10 seconds
		env.getCheckpointConfig().setCheckpointInterval(10_000);
		env.setParallelism(1);
		// use event time for the application
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(500L);

		// ingest sensor stream
		DataStream<SensorReading> outOfOrderReadings = env
				// SensorSource generates random temperature readings
				.addSource(new SensorSource())
				// shuffle timestamps by max 7 seconds to generate late data
				.map(new TimestampShuffler(7 * 1000))
				// assign timestamps and watermarks with an offset of 5 seconds
				.assignTimestampsAndWatermarks(new SensorTimeAssigner());
		// outOfOrderReadings.printToErr();
		// Different strategies to handle late records.
		// Select and uncomment on of the lines below to demonstrate a strategy.
		OutputTag<SensorReading> lateReadingsOutput = new OutputTag<SensorReading>("late-readings") {
			private static final long serialVersionUID = 1L;

		};
		// 1. Filter out late readings (to a side output) using a ProcessFunction
		// 小于水印时间的乱序数据分流
		// filterLateReadings(outOfOrderReadings);
		SingleOutputStreamOperator<SensorReading> filteredReadings = outOfOrderReadings
				.process(new ProcessFunction<SensorReading, SensorReading>() {
					@Override
					public void processElement(SensorReading r,
							ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out)
							throws Exception {
						// compare record timestamp with current watermark
						if (r.timestamp < ctx.timerService().currentWatermark()) {
							// this is a late reading => redirect it to the side output
							ctx.output(lateReadingsOutput, r);
						} else {
							out.collect(r);
						}
					}
				});
		// retrieve late readings
		DataStream<SensorReading> lateReadings = filteredReadings.getSideOutput(lateReadingsOutput);
		// filteredReadings.printToErr();
//		lateReadings.map(r -> "*** late reading *** " + r.id).printToErr();

		// 2. Redirect late readings to a side output in a window operator
		SingleOutputStreamOperator<Tuple3<String, Long, Integer>> countPer10Secs = outOfOrderReadings
				.keyBy(new KeySelector<SensorReading, String>() {
					@Override
					public String getKey(SensorReading value) throws Exception {
						return value.id;
					}
				}).timeWindow(Time.seconds(10))
				// emit late readings to a side output
				.sideOutputLateData(lateReadingsOutput)
				// count readings per window
				.process(new ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {

					@Override
					public void process(String id,
							ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>.Context ctx,
							Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> out)
							throws Exception {
						Integer cnt =((ArrayList<SensorReading>)elements).size();
//						Integer cnt = elements.count(f -> true);
						out.collect(new Tuple3(id, ctx.window().getEnd(), cnt));
					}

				});
//		  countPer10Secs
//	      .getSideOutput(lateReadingsOutput)
//	      .map(r -> "*** late reading *** " + r.id)
//	      .print();

	    // print results
//	    countPer10Secs.print();
	    // 3. Update results when late readings are received in a window operator
		  /** Count reading per tumbling window and update results if late readings are received.
		    * Print results. */
		SingleOutputStreamOperator<Tuple4<String, Long, Integer,String>> countPer10Secs2 = outOfOrderReadings
				.keyBy(new KeySelector<SensorReading, String>() {
					@Override
					public String getKey(SensorReading value) throws Exception {
						return value.id;
					}
				}).timeWindow(Time.seconds(10))
				   // process late readings for 5 additional seconds
			      .allowedLateness(Time.seconds(5))
			      // count readings and update results if late readings arrive
			      .process(new ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer,String>, String, TimeWindow>() {

					@Override
					public void process(String id,
							ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer,String>, String, TimeWindow>.Context ctx,
							Iterable<SensorReading> elements, Collector<Tuple4<String, Long, Integer,String>> out)
							throws Exception {
						Integer cnt =((ArrayList<SensorReading>)elements).size();
//						Integer cnt = elements.count(f -> true);
					    // state to check if this is the first evaluation of the window or not.
						ValueState<Boolean> isUpdate= ctx.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", TypeInformation.of(Boolean.class)));
						 if (isUpdate.value()==null||!isUpdate.value()) {
						      // first evaluation, emit first result
						      out.collect(new Tuple4(id, ctx.window().getEnd(), cnt, "first"));
						      isUpdate.update(true);
						    } else {
						      // not the first evaluation, emit an update
						      out.collect(new Tuple4(id, ctx.window().getEnd(), cnt, "update"));
						    }
					}

				});
		  countPer10Secs2
	      .print();
		env.execute();
	}

	/**
	 * monotonically increases 1s时间内温度都是单调题赠就告警 Emits a warning if the temperature
	 * of a sensor monotonically increases for 1 second (in processing time).
	 */
	public static class TimestampShuffler implements MapFunction<SensorReading, SensorReading> {
		private Random rand = new Random();
		private Integer maxRandomOffset;

		public TimestampShuffler(Integer maxRandomOffset) {
			super();
			this.maxRandomOffset = maxRandomOffset;
		}

		@Override
		public SensorReading map(SensorReading r) throws Exception {
			long shuffleTs = r.timestamp + rand.nextInt(maxRandomOffset);
			return new SensorReading(r.id, shuffleTs, r.temperature);
		}

	}

	

}
