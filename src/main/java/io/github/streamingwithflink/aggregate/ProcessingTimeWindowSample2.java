 package io.github.streamingwithflink.aggregate;

import java.util.HashMap;
import java.util.Random;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import io.github.streamingwithflink.util.MetricTimeHelper;
/**
 * https://www.infoq.cn/article/iGyoaq8_G3io63sqPglE
 * Apache Flink 零基础入门（三）：DataStream API 编程
 * @author guoxian
 * @date 2021/03/02
 */
public class ProcessingTimeWindowSample2 {
	    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
	        private volatile boolean isRunning = true;

	        @Override
	        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
	            Random random = new Random();
	            while (isRunning) {
	            	long timeFlag = MetricTimeHelper.getTimeStampFlagByStep(System.currentTimeMillis(), "1");
	        		String timeDtf = MetricTimeHelper.formatTime(timeFlag);
//	                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
	                Thread.sleep(100);
	                String key = "类别" + (char) ('A' + random.nextInt(3))+timeDtf;
	                int value = random.nextInt(10) + 1;

//	                System.out.println(String.format("Emits\t(%s, %d)", key, value));
	                ctx.collect(new Tuple2<>(key, value));
	            }
	        }

	        @Override
	        public void cancel() {
	            isRunning = false;
	        }
	    }

	    public static void main(String[] args) throws Exception {
	        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	        env.setParallelism(2);

	        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
	        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);
	        //(类别A,12)  (类别B,17) (类别C,31)
//	        keyedStream.sum(1).printToErr();
//	        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
//	            @Override
//	            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//	                return stringIntegerTuple2.f0;
//	            }
//	        }).printToErr();
	        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
	            @Override
	            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//	            	 return stringIntegerTuple2.f0;
	                return "";
	            }
	        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
	            @Override
	            public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
	                accumulator.put(value.f0, value.f1);
	                return accumulator;
	            }
	        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
	            @Override
	            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
	                  // 每个类型的商品成交量
	                  System.err.println(value);
	                  // 商品成交总量                
	                  System.out.println(value.values().stream().mapToInt(v -> v).sum());
	            }
	        });

	        env.execute();
	    }
	}
