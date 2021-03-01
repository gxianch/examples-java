 package io.github.streamingwithflink.processfuncdemo;

 /**
  * 存储在状态中的数据类型
  */
 public class CountWithTimestamp {

 	public String key;           // 存储key
 	public long count;           // 存储计数值
 	public long lastModified;    // 最后一次修改时间
	@Override
	public String toString() {
		return "CountWithTimestamp [key=" + key + ", count=" + count + ", lastModified=" + lastModified +", lastModified+60=" + (lastModified+60000) + "]";
	}
 	
 }