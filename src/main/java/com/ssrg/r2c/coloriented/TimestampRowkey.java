package com.ssrg.r2c.coloriented;

public class TimestampRowkey {

	public long first;
	public long last;
	
	public boolean isEmpty() {
		if (first == 0 && last == 0) {
			return true;
		}
		
		return false;
	}
}
