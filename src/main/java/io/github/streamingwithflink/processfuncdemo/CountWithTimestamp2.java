 package io.github.streamingwithflink.processfuncdemo;

 /**
  * 存储在状态中的数据类型
  */
 public class CountWithTimestamp2 {

 	public String key;           // 存储key
 	public long count;           // 存储计数值
	@Override
	public String toString() {
		return "CountWithTimestamp2 [key=" + key + ", count=" + count + "]";
	}
 	
 }