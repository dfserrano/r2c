package com.ssrg.r2c.rdms.metadata;


public class IndexStatus {

	private String name;
	private boolean unique;
	private String column;
	private int cardinality;
	private boolean isNull;

	public IndexStatus(String name, boolean unique, String column,
			int cardinality, boolean isNull) {
		this.name = name;
		this.unique = unique;
		this.column = column;
		this.cardinality = cardinality;
		this.isNull = isNull;
	}

	public String getName() {
		return name;
	}

	public boolean isUnique() {
		return unique;
	}

	public String getColumn() {
		return column;
	}

	public int getCardinality() {
		return cardinality;
	}

	public boolean isNull() {
		return isNull;
	}

	@Override
	public String toString() {
		return "IndexStatus [name=" + name + ", unique=" + unique + ", column="
				+ column + ", cardinality=" + cardinality + ", null=" + isNull
				+ "]";
	}
}
