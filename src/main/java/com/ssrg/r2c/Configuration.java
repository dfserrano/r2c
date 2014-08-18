package com.ssrg.r2c;

public class Configuration {

	private int timestampBytePrecision = 5;
	private String tableSeparator = ".";
	private int usageThreshold = 1;
	private int maxExtLevel = 2;
	private String extColFamilyDelimiter = "-";
	private String clonedTableSeparator = "_";

	public int getUsageThreshold() {
		return usageThreshold;
	}

	public void setUsageThreshold(int usageThreshold) {
		this.usageThreshold = usageThreshold;
	}

	public int getMaxExtLevel() {
		return maxExtLevel;
	}

	public void setMaxExtLevel(int maxExtLevel) {
		this.maxExtLevel = maxExtLevel;
	}

	public String getExtColFamilyDelimiter() {
		return extColFamilyDelimiter;
	}

	public void setExtColFamilyDelimiter(String extColFamilyDelimiter) {
		this.extColFamilyDelimiter = extColFamilyDelimiter;
	}

	public String getClonedTableSeparator() {
		return clonedTableSeparator;
	}

	public void setClonedTableSeparator(String clonedTableSeparator) {
		this.clonedTableSeparator = clonedTableSeparator;
	}

	public int getTimestampBytePrecision() {
		return timestampBytePrecision;
	}

	public void setTimestampBytePrecision(int timestampBytePrecision) {
		this.timestampBytePrecision = timestampBytePrecision;
	}

	public String getTableSeparator() {
		return tableSeparator;
	}

	public void setTableSeparator(String tableSeparator) {
		this.tableSeparator = tableSeparator;
	}

}
