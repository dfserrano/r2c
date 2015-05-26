package com.ssrg.r2c.util;

import java.util.Calendar;

import com.ssrg.r2c.Configuration;

public class Utils {

	public static long bytesToLong(byte[] b) {
		long r = 0;

		int shift = 8 * (b.length - 1);
		for (int i = 0; i < b.length; i++) {
			r += (b[i] & 0xFF) << shift;
			shift -= 8;
		}

		return r;
	}
	
	public static String bytesToBinaryString(byte[] b) {
		StringBuffer sb = new StringBuffer();
		for (byte by : b) {
		    sb.append(Integer.toBinaryString(by & 255 | 256).substring(1));
		}
		return sb.toString();
	}
	
	public static long roundDownTimestamp(long timestamp, Configuration conf) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(timestamp);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		if (conf.getTimestampBytePrecision() == Configuration.TIMESTAMP_PRECISION_DAY
				|| conf.getTimestampBytePrecision() == Configuration.TIMESTAMP_PRECISION_MONTH) {
			c.set(Calendar.HOUR, 0);
		}

		if (conf.getTimestampBytePrecision() == Configuration.TIMESTAMP_PRECISION_MONTH) {
			c.set(Calendar.DAY_OF_MONTH, 1);
		}

		return c.getTimeInMillis();
	}
}
