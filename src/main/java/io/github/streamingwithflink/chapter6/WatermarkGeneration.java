package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

/**
 * 改造自example-scala分支
 * @author guoxian
 * @date 2021/03/05
 */
public class WatermarkGeneration {
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// checkpoint every 10 seconds
		env.getCheckpointConfig().setCheckpointInterval(10_000);

		// use event time for the application
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(1000L);

		// ingest sensor stream
		DataStream<SensorReading> readings = env
				// SensorSource generates random temperature readings
				.addSource(new SensorSource());

//		DataStream<SensorReading> readingsWithPeriodicWMs=	readings
//				// assign timestamps and periodic watermarks
//				.assignTimestampsAndWatermarks(new PeriodicAssigner());
//	    readingsWithPeriodicWMs.printToErr();
		
		DataStream<SensorReading> readingsWithPunctuatedWMs= readings
				      // assign timestamps and punctuated watermarks
				      .assignTimestampsAndWatermarks(new PunctuatedAssigner());
	    readingsWithPunctuatedWMs.print();
	    
		env.execute("Assign timestamps and generate watermarks");
	}

	/**
	 * Assigns timestamps to records and provides watermarks with a 1 minute
	 * out-of-ourder bound when being asked.
	 */
	public static class PeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {
		Long bound = 60 * 1000L;
		Long maxTs = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(SensorReading r, long previousTS) {
			// update maximum timestamp
			maxTs = Long.max(maxTs, r.timestamp);
			// maxTs = maxTs.max(r.timestamp);
			// return record timestamp
			return r.timestamp;
		}

		@Override
		public Watermark getCurrentWatermark() {
			// TODO Auto-generated method stub
			return new Watermark(maxTs - bound);
		}
	}

	/**
	 * Assigns timestamps to records and emits a watermark for each reading with
	 * sensorId == "sensor_1".
	 */
	public static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {
		Long bound = 60 * 1000L;

		@Override
		public long extractTimestamp(SensorReading r, long previousElementTimestamp) {
			return r.timestamp;
		}

		@Override
		public Watermark checkAndGetNextWatermark(SensorReading r, long extractedTS) {
			if (r.id == "sensor_1") {
				// emit watermark if reading is from sensor_1
				return new Watermark(extractedTS - bound);
			} else {
				// do not emit a watermark
				return null;
			}
		}

	}

}
