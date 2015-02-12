package com.ssrg.r2c;

public class Configuration {

	public static final int TIMESTAMP_PRECISION_HOUR = 1; 
	public static final int TIMESTAMP_PRECISION_DAY = 2; 
	public static final int TIMESTAMP_PRECISION_MONTH = 3; 
	
	public static final int TIMESTAMP_NUM_BYTE_HOUR = 3;
	public static final int TIMESTAMP_NUM_BYTE_DAY = 4;
	public static final int TIMESTAMP_NUM_BYTE_MONTH = 4; // Complete
	
	private int timestampBytePrecision = TIMESTAMP_PRECISION_HOUR;
	private String tableSeparator = ".";
	private int usageThreshold = 1;
	private int maxExtLevel = 1; // 2
	private String extColFamilyDelimiter = "-";
	private String clonedTableSeparator = "_";
	private int rowSample = -1;
	
	private String hbaseHostname = "localhost";
	private String zookeeperQuorum = "localhost";
	private int zookeeperPort = 2181;

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
	
	public int getTimestampNumBytesPrecision() {
		if (timestampBytePrecision == TIMESTAMP_PRECISION_HOUR) {
			return TIMESTAMP_NUM_BYTE_HOUR;
		} else if (timestampBytePrecision == TIMESTAMP_PRECISION_DAY) {
			return TIMESTAMP_NUM_BYTE_DAY;
		} else if (timestampBytePrecision == TIMESTAMP_PRECISION_MONTH) {
			return TIMESTAMP_NUM_BYTE_MONTH;
		}
		
		return 3;
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

	public int getRowSample() {
		return rowSample;
	}

	public String getHbaseHostname() {
		return hbaseHostname;
	}

	public void setHbaseHostname(String hbaseHostname) {
		this.hbaseHostname = hbaseHostname;
	}

	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}

	public int getZookeeperPort() {
		return zookeeperPort;
	}

	public void setZookeeperPort(int zookeeperPort) {
		this.zookeeperPort = zookeeperPort;
	}
}
