package com.ssrg.r2c.util;

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
}
