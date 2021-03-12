package io.github.streamingwithflink.chapter6;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import io.github.streamingwithflink.util.TimeHelper;

/**
 * 改造自example-scala分支
 * @author guoxian
 * @date 2021/03/05
 */
public class WindowFunctions {
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// use event time for the application
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(1000L);
		// ingest sensor stream
		DataStream<SensorReading> sensorData = env
				// SensorSource generates random temperature readings
				.addSource(new SensorSource())
				// assign timestamps and watermarks which are required for event time
				.assignTimestampsAndWatermarks(new SensorTimeAssigner());
		//每个窗口时间最小温度
		DataStream<Tuple2<String, Double>> minTempPerWindow = sensorData
				.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
					@Override
					public Tuple2 map(SensorReading r) throws Exception {
						return new Tuple2(r.id, r.temperature);
					}
				}).keyBy(new KeySelector<Tuple2<String, Double>, String>() {
					@Override
					public String getKey(Tuple2<String, Double> r) throws Exception {
						return r.f0;
					}
				}).timeWindow(Time.seconds(15)).reduce(new ReduceFunction<Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> reduce(Tuple2<String, Double> r1, Tuple2<String, Double> r2)
							throws Exception {
						return new Tuple2<String, Double>(r1.f0, Double.min(r1.f1, r2.f1));
					}
				});
		// minTempPerWindow.printToErr();
		//每个窗口时间平均温度
		DataStream<Tuple2<String, Double>> avgTempPerWindow = sensorData
				.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
					@Override
					public Tuple2 map(SensorReading r) throws Exception {
						return new Tuple2(r.id, r.temperature);
					}
				}).keyBy(new KeySelector<Tuple2<String, Double>, String>() {
					@Override
					public String getKey(Tuple2<String, Double> r) throws Exception {
						return r.f0;
					}
				}).timeWindow(Time.seconds(15)).aggregate(
						new AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
							@Override
							public Tuple3<String, Double, Integer> createAccumulator() {
								// TODO Auto-generated method stub
								return new Tuple3<String, Double, Integer>("", 0.0, 0);
							}

							@Override
							public Tuple3<String, Double, Integer> add(Tuple2<String, Double> in,
									Tuple3<String, Double, Integer> acc) {
								return new Tuple3<String, Double, Integer>(in.f0, in.f1 + acc.f1, 1 + acc.f2);
							}

							@Override
							public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
								return new Tuple2<String, Double>(acc.f0, acc.f1 / acc.f2);
							}

							@Override
							public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1,
									Tuple3<String, Double, Integer> acc2) {
								return new Tuple3<String, Double, Integer>(acc1.f0, acc1.f1 + acc2.f1,
										acc1.f2 + acc2.f2);
							}
						});
		// avgTempPerWindow.printToErr();
		//每个窗口时间的最小和最大温度
		DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData.keyBy(new KeySelector<SensorReading, String>() {
			@Override
			public String getKey(SensorReading r) throws Exception {
				return r.id;
			}
		}).timeWindow(Time.seconds(5))
				.process(new HighAndLowTempProcessFunction());
//		minMaxTempPerWindow.printToErr();
		
		
		
		DataStream<MinMaxTemp> minMaxTempPerWindow2 =sensorData.map(new MapFunction<SensorReading, Tuple3<String, Double, Double>>() {

			@Override
			public Tuple3<String, Double, Double> map(SensorReading r) throws Exception {
				// TODO Auto-generated method stub
				 return new Tuple3<String, Double, Double>(r.id, r.temperature, r.temperature);
			}
		}).keyBy(new KeySelector<Tuple3<String, Double, Double>, String>() {
					@Override
					public String getKey(Tuple3<String, Double, Double> r) throws Exception {
						return r.f0;
					}
				})
		  .timeWindow(Time.seconds(5))
		  .reduce(new ReduceFunction<Tuple3<String,Double,Double>>() {

			@Override
			public Tuple3<String, Double, Double> reduce(Tuple3<String, Double, Double> r1,
					Tuple3<String, Double, Double> r2) throws Exception {
				// TODO Auto-generated method stub
				 return new Tuple3<String, Double, Double>(r1.f0, Math.min(r1.f1, r2.f1),  Math.max(r1.f2, r2.f2));
			}
		},
			        // finalize result in ProcessWindowFunction
			        new AssignWindowEndProcessFunction());
		  minMaxTempPerWindow2.printToErr();
		env.execute();
	}

	public static class MinMaxTemp {
		public String id;
		public Double min;
		public Double max;
		public Long endTs;

		public MinMaxTemp(String id, Double min, Double max, Long endTs) {
			super();
			this.id = id;
			this.min = min;
			this.max = max;
			this.endTs = endTs;
		}

		@Override
		public String toString() {
			return "MinMaxTemp [id=" + id + ", min=" + min + ", max=" + max + ", endTs=" + endTs + " "
					+ TimeHelper.formatTime(endTs) + "]";
		}

	}

	/**
	 * A ProcessWindowFunction that computes the lowest and highest temperature
	 * reading per window and emits a them together with the end timestamp of the
	 * window.
	 */
	public static class HighAndLowTempProcessFunction
			extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {

		@Override
		public void process(String key,
				ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>.Context ctx,
				Iterable<SensorReading> vals, Collector<MinMaxTemp> out) throws Exception {
			List<Double> temps = ((ArrayList<SensorReading>) vals).stream().map(m -> m.temperature)
					.collect(Collectors.toList());
			long windowEnd = ctx.window().getEnd();
			out.collect(new MinMaxTemp(key, Collections.min(temps), Collections.max(temps), windowEnd));
		}

	}
	
	public static class AssignWindowEndProcessFunction
	extends ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow> {

		@Override
		public void process(String key,
				ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow>.Context ctx,
				Iterable<Tuple3<String, Double, Double>> minMaxIt, Collector<MinMaxTemp> out) throws Exception {
//			minMaxIt只有一个值
			Tuple3<String, Double, Double> minMax = minMaxIt.iterator().next();
			long windowEnd = ctx.window().getEnd();
		    out.collect(new MinMaxTemp(key, minMax.f1, minMax.f2, windowEnd));
		}
		
	}
}
