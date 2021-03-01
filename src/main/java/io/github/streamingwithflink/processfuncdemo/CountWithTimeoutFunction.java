package io.github.streamingwithflink.processfuncdemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutFunction
		extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

	/** 由这个处理函数负责维护的状态 */
	private ValueState<CountWithTimestamp> state;

	// 首先获得由这个处理函数（process function）维护的状态
	// 通过 RuntimeContext 访问Flink的keyed state
	@Override
	public void open(Configuration parameters) throws Exception {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
	}

	// 对于在输入流中接收到的每一个事件，此函数就会被调用以处理该事件
	// 对于每个记录，KeyedProcessFunction递增计数器并设置最后修改时间戳
	@Override
	public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out)
			throws Exception {

		// 获取当前的计数
		CountWithTimestamp current = state.value();
		if (current == null) {
			current = new CountWithTimestamp();
			current.key = value.f0;
		}

		// 更新状态计数值
		current.count++;

		// 设置该状态的时间戳为记录的分配的事件时间时间时间戳
		if (ctx != null) {
			current.lastModified = ctx.timestamp();
		}

		// 将状态写回
		state.update(current);
//		CountWithTimestamp [key=good, count=1, lastModified=1614561933663, lastModified+60000=1614561993663]
//		CountWithTimestamp [key=good, count=2, lastModified=1614561933677, lastModified+60000=1614561993677]
//		CountWithTimestamp [key=good, count=3, lastModified=1614561933684, lastModified+60000=1614561993684]
//		CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60000=1614561993685]
		System.err.println(current);
		// 从当前事件时间开始安排下一个计时器60秒   onTimer(timestamp) 触发的时间 
		ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
	}

	// 如果一分钟内没有进一步的更新，则发出 key/count对
//	ctx.timerService().registerEventTimeTimer(current.lastModified + 60000)的回调函数
//	触发时间  timestamp=current.lastModified + 60000
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
		
		// 获取调度此计时器的key的状态
		CountWithTimestamp result = state.value();
//		timestamp值等于processElement函数中ctx.timerService().registerEventTimeTimer(current.lastModified + 60000)的值
//		每次进来的state值是一样的，是最后一条记录  CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60=1614561993685]

//		timestamp=(current.lastModified + 60000):1614561993663
//		CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60=1614561993685]
//		timestamp=(current.lastModified + 60000):1614561993677
//		CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60=1614561993685]
//		timestamp=(current.lastModified + 60000):1614561993684
//		CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60=1614561993685]
//		timestamp=(current.lastModified + 60000):1614561993685
//		CountWithTimestamp [key=good, count=4, lastModified=1614561933685, lastModified+60=1614561993685]
		System.out.println("timestamp=(current.lastModified + 60000):"+timestamp);
		System.out.println(result);
//		前三次timestamp与result.lastModified + 60000不相等，只有最后一次才相等都是1614561993685
		// 检查这是一个过时的计时器还是最新的计时器        timestamp等到一分钟的最后一条记录processElement处理才输出
		if (timestamp == result.lastModified + 60000) {
			// 超时时发出状态
			out.collect(new Tuple2<String, Long>(result.key, result.count));
		}
	}
}