package io.github.streamingwithflink.processfuncdemo;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricTimeHelper
 * 
 * @ApplicationName: shsnc-fk-new
 * @Title: MetricTimeHelper.java
 * @Package: com.shsnc.fk.common.datahandler.metrictime
 * @author: Shier
 * @date: 2020年11月24日 上午11:13:23
 * @version: V1.0
 */
public class MetricTimeHelper {

	public static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private final static Logger LOG = LoggerFactory.getLogger(MetricTimeHelper.class);

	/**
	 * getNowTime 获取当前时间
	 *
	 * @return
	 * @return Timestamp
	 **/
	public static String getNowTimeStr() {
		try {
			return DTF.format(LocalDateTime.now());
		}
		catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return "1970-01-01 00:00:00";
		}
	}

	/**
	 * 时间戳计算统计时间
	 * 
	 * @param time
	 *            时间戳
	 * @param counter
	 *            统计周期
	 * @param step
	 *            取整长度
	 * @return String 返回类型
	 */
	public static long getTargetTime(long time, long counter, int step) {
		return ((time / counter) + step) * counter;
	}

	/**
	 * 判断一个整数是否有n位
	 * 
	 * @param number
	 * @param n
	 * @return boolean 返回类型
	 */
	private static int lengthOfNumber(long number) {
		int length = 1;
		while (number >= 10000) {
			number /= 10000;
			length += 4;
		}
		return number >= 1000 ? length + 3 : (number >= 100 ? length + 2 : (number >= 10 ? length + 2 : length));
	}

	/**
	 * 根据step来返回当前数据属于的时间标签
	 *
	 * @param time
	 *            当前数据的时间
	 * @param step
	 *            0表示实时；5/10/20分别对应分钟
	 * @return 时间标识
	 */
	public static long getTimeStampFlagByStep(long timestamp, String step) {
		int stepInt = Integer.valueOf(step);
		if (stepInt <= 0) {
			return timestamp;
		}
		return getTargetTime(timestamp, stepInt * 60 * 1000, 1);
	}

	/**
	 * 时间戳转标准格式 默认为13位带毫秒
	 * 
	 * @param time
	 * @param second
	 * @return String 返回类型
	 */
	public static String formatTime(long time) {
		return DTF.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
	}

	/**
	 * yyyy-MM-dd HH:mm:ss 获取时间戳
	 * 
	 * @param time
	 * @return Long 返回类型
	 */
	public static long getTimeStamp(Object time) {
		if (time instanceof Long) {
			return formatTimestamp((long) time);
		}
		else if (time instanceof Integer) {
			return formatTimestamp(((Integer) time).longValue());
		}
		else {
			String timeStr = time.toString();
			int dtfIndex = timeStr.indexOf("-");
			if (dtfIndex > -1) {
				int pointIndex = timeStr.indexOf(".");
				if (pointIndex > -1) {
					timeStr = timeStr.substring(0, pointIndex);
				}
				return formatTimestamp(Timestamp.valueOf(LocalDateTime.parse(timeStr, DTF)).getTime());
			}
			return formatTimestamp(Long.valueOf(timeStr));
		}
	}

	/**
	 * 格式化时间为13位long时间戳
	 * 
	 * @param timestamp
	 * @return long 返回类型
	 */
	private static long formatTimestamp(long timestamp) {
		int numberLen = lengthOfNumber(timestamp) - 13;
		if (numberLen == 0) {
			return timestamp;
		}
		if (numberLen > 0) {
			switch (numberLen) {
				case 1:
					return timestamp /= 10;
				case 2:
					return timestamp /= 100;
				case 3:
					return timestamp /= 1000;
				default:
					for (int i = 0; i < numberLen; i++) {
						timestamp /= 10;
					}
					return timestamp;
			}
		}
		else {
			switch (numberLen) {
				case -1:
					return timestamp *= 10;
				case -2:
					return timestamp *= 100;
				case -3:
					return timestamp *= 1000;
				default:
					for (int i = 0; i < -numberLen; i++) {
						timestamp *= 10;
					}
					return timestamp;
			}
		}
	}
}
