 package io.github.streamingwithflink.processfuncdemo;

 import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
 /**
  * https://zhuanlan.zhihu.com/p/130708277
  * 可以将ProcessFunction看作是一个具有key state和定时器(timer)访问权的FlatMapFunction。对于在输入流中接收到的每一个事件，此函数就会被调用以处理该事件。
对于容错状态，ProcessFunction 可以通过 RuntimeContext 访问Flink的keyed state，这与其他有状态函数访问keyed state的方式类似。
定时器可让应用程序对在处理时间和事件时间中的变化进行响应。每次调用 processElement(...)函数时都可以获得一个Context对象，通过该对象可以访问元素的事件时间（event time）时间戳以及 TimerService。可以使用TimerService为将来的事件时间/处理时间实例注册回调。对于事件时间计时器，当当前水印被提升到或超过计时器的时间戳时，将调用onTimer(…)方法，而对于处理时间计时器，当挂钟时间达到指定时间时，将调用onTimer(…)方法。在调用期间，所有状态的范围再次限定为创建定时器所用的key，从而允许定时器操作keyed state。
如果想要在流处理过程中访问keyed state和定时器，就必须在一个keyed stream上应用ProcessFunction函数，代码如下：
stream.keyBy(...).process(new MyProcessFunction())

  * 在下面的示例中，KeyedProcessFunction维护每个key的计数，并在每过一分钟(以事件时间)而未更新该key时，发出一个key/count对：
把计数、key和最后修改时间戳（last-modification-timestamp）存储在一个ValueState中, ValueState的作用域是通过key隐式确定的。
对于每个记录，KeyedProcessFunction递增计数器并设置最后修改时间戳。
该函数还安排了一个一分钟后的回调(以事件时间)。
在每次回调时，它根据存储的计数的最后修改时间检查回调的事件时间时间戳，并在它们匹配时发出key/count（即，在该分钟内没有进一步的更新）。
【示例】维护数据流中每个key的计数，并在每过一分钟(以事件时间)而未更新该key时，发出一个key/count对。


https://www.infoq.cn/article/zvC15XOtpP5X5BRXHWUz   
崔星灿 Apache Flink 进阶（二）：时间属性深度解析

  * @author guoxian
  * @date 2021/02/26
  */
public class StreamingJob2 {
	    public static void main(String[] args) throws Exception {
		// 设置流执行环境
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// 默认情况下，Flink将使用处理时间。要改变这个，可以设置时间特征:
	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	        env.setParallelism(1);
	        // 源数据流
	        DataStream<Tuple2<String, String>> stream = env
	                .fromElements("good good study","day day up","you see see you","good good study")
	                .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
	                    @Override
	                    public void flatMap(String line, Collector<Tuple2<String, String>> collector) throws Exception {
	                        for(String word : line.split("\\W+")){
	                            collector.collect(new Tuple2<>(word,"1"));
	                        }
	                    }
	                });
//	        (good,1)(good,1)(study,1)(day,1)(day,1)(up,1)(you,1)(see,1)(see,1)(you,1)
//	        stream.printToErr();
		// 因为模拟数据没有时间戳，所以用此方法添加时间戳和水印
	        DataStream<Tuple2<String, String>> withTimestampsAndWatermarks =
	                stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, String>>() {
	                    @Override
	                    public long extractAscendingTimestamp(Tuple2<String, String> element) {
	                        return System.currentTimeMillis();
	                    }
	                });
		// 在keyed stream上应用该处理函数
		DataStream<Tuple2<String, Long>> result = withTimestampsAndWatermarks.keyBy(0).process(new MetricKeyedProcessOnTimerFunction());
	
		// 输出查看 (good,2)(study,1)(day,2)(see,2)(up,1)(you,2)
	        result.printToErr();

		// 执行流程序
		env.execute("Process Function");
	    }
	}