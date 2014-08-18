package com.ssrg.r2c.coloriented;

public class RecordValue {

	private String valueString;
	private byte[] valueBytes;
	
	public RecordValue(String v1, byte[] v2) {
		valueString = v1;
		valueBytes = v2;
	}

	public String getValueString() {
		return valueString;
	}

	public byte[] getValueBytes() {
		return valueBytes;
	}
}
