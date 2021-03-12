package io.github.streamingwithflink.chapter6;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;

import io.github.streamingwithflink.processfuncdemo.CountWithTimestamp2;
import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.TimeHelper;

/**
 * 改造自example-scala分支
 * @author guoxian
 * @date 2021/03/05
 */
public class ProcessFunctionTimers {
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// checkpoint every 10 seconds
		env.getCheckpointConfig().setCheckpointInterval(10_000);
		env.setParallelism(1);
		// use event time for the application
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(1000L);

		// ingest sensor stream
		DataStream<SensorReading> readings = env
				// SensorSource generates random temperature readings
				.addSource(new SensorSource());
		readings
				// key by sensor id
				.keyBy(new KeySelector<SensorReading, String>() {
					@Override
					public String getKey(SensorReading value) throws Exception {
						return value.id;
					}
				})
				// apply ProcessFunction to monitor temperatures
				.process(new TempIncreaseAlertFunction()).printToErr();
		env.execute("Monitor sensor temperatures.");
	}

	/**
	 *  monotonically increases 1s时间内温度都是单调题赠就告警
	 * Emits a warning if the temperature of a sensor monotonically increases for 1
	 * second (in processing time).
	 */
	public static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {
		/** 由这个处理函数负责维护的状态 */
		private ValueState<Double> lastTemp;
		private ValueState<Long> currentTimer;

		@Override
		public void open(Configuration parameters) throws Exception {
			lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Double.class));
			currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
		}

		@Override
		public void processElement(SensorReading r, KeyedProcessFunction<String, SensorReading, String>.Context ctx,
				Collector<String> out) throws Exception {
			// get previous temperature
			Double prevTemp = lastTemp.value();
			if (prevTemp == null) {
				prevTemp = 0.0;
			}
			// update last temperature
			lastTemp.update(r.temperature);
			Long curTimerTimestamp = currentTimer.value();
			if (curTimerTimestamp == null) {
				curTimerTimestamp = 0L;
			}
			if (prevTemp == 0.0) {
				// first sensor reading for this key.
				// we cannot compare it with a previous value.
			} else if (r.temperature < prevTemp) {
				// temperature decreased. Delete current timer.
				System.err.println(r.id+"#"+r.temperature+"<"+prevTemp+":"+TimeHelper.formatTime(curTimerTimestamp));
				ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
				currentTimer.clear();
			} else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
				// temperature increased and we have not set a timer yet.
				// set timer for now + 1 second
				Long timerTs = ctx.timerService().currentProcessingTime() + 1000;
				System.err.println(r.id+"#"+r.temperature+">"+prevTemp+"&&"+TimeHelper.formatTime(curTimerTimestamp)+":"+TimeHelper.formatTime(timerTs));
				ctx.timerService().registerProcessingTimeTimer(timerTs);
				// remember current timer
				currentTimer.update(timerTs);
			}
		}

		@Override
		public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
			out.collect("Temperature of sensor '" + ctx.getCurrentKey() +" '" +TimeHelper.formatTime(ts)+"' monotonically increased for 1 second.");
			// reset current timer
			currentTimer.clear();
		}
	}

}
