package com.ssrg.r2c.rdms.metadata;

public class ColumnStatus {

	private String name;
	private boolean isPrimaryKey;
	private String type;

	public ColumnStatus(String name, boolean isPk, String type) {
		super();
		this.name = name;
		this.isPrimaryKey = isPk;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return "ColumnStatus [name=" + name + ", isPrimaryKey=" + isPrimaryKey
				+ "]";
	}

}
