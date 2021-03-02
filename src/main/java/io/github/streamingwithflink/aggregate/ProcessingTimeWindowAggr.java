 package io.github.streamingwithflink.aggregate;

import java.util.HashMap;
import java.util.Random;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import io.github.streamingwithflink.util.TimeHelper;

/**
 * 实现fuhezhibiao的demo用例
 *  .window(ProcessingTimeSessionWindows.withGap(Time.seconds(Integer.valueOf(1))))
    .aggregate(new MergeAggregateFunc())
	       
 * @author guoxian
 * @date 2021/03/02
 */
public class ProcessingTimeWindowAggr {
	    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
	        private volatile boolean isRunning = true;

	        @Override
	        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
	            Random random = new Random();
	            while (isRunning) {
	            	//统计间隔1分钟,如果是5分钟呢
	            	long timeFlag = TimeHelper.getTimeStampFlagByStep(System.currentTimeMillis(), "1");
	        		String timeDtf = TimeHelper.formatTime(timeFlag);
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
//	 keyBy按分钟        类别A2021-03-02 09:04:00            类别B2021-03-02 09:05:00
	        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);
	        //(类别A,12)  (类别B,17) (类别C,31)
	        //滚动处理时间窗口(tumbling processing-time windows)  key
	        //有问题，keyBy按分钟分组和滚动时间一分钟不同步，导致keyBy不是按一分钟处理的，keyBy分两次输出了 ,
//	        1> {类别A2021-03-02 09:07:00=2193}
//	        1> {类别C2021-03-02 09:07:00=2127}
//	        2> {类别C2021-03-02 09:06:00=18}
//	        2> {类别B2021-03-02 09:07:00=2161}
//	        2> {类别B2021-03-02 09:07:00=10}
//	        1> {类别C2021-03-02 09:07:00=4}
//	        keyedStream
//	        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) 
//	        .aggregate(new MergeAggregateFunc()).printToErr();
	      
	        
//	        1> {类别A2021-03-02 09:15:00=364}
//	        1> {类别A2021-03-02 09:15:00=1690}
//	        1> {类别C2021-03-02 09:15:00=2206}
//	        1> {类别B2021-03-02 09:15:00=2279}
//	        ProcessingTimeSessionWindows.withGap(Time.seconds(1))在一个key中也有多次输出的bug,比如key:类别A2021-03-02 09:15:00输出2个值,
//	        	因为处理时间可能超过1s就会断开处理
//	        ProcessingTimeSessionWindows.withGap(Time.minutes(1))能解决上述问题，
//	        key是每分钟一个key,等这个key一分钟后不再出现再处理，key的下一分钟作为处理时间，一个key不会断开处理2次
//	                         只是结果输出会延时一分钟09:30:00输出的是09:29:00的结果 {类别B2021-03-02 09:29:00=2198} {类别A2021-03-02 09:29:00=2239}
	        keyedStream
//	        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
	        .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
	        .aggregate(new MergeAggregateFunc()).printToErr();
	        
	        
	       /* keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
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
*/
	        env.execute();
	    }
	}
