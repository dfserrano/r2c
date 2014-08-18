package com.ssrg.r2c.rdms.metadata;

public class ForeignKeyStatus {

	private String name;
	private String table;
	private String column;
	private String refTable;
	private String refColumn;
	
	private Type type;
	
	public enum Type { ONE, MANY }
	
	public ForeignKeyStatus(String name, String table, String column,
			String refTable, String refColumn) {
		super();
		this.name = name;
		this.table = table;
		this.column = column;
		this.refTable = refTable;
		this.refColumn = refColumn;
	}

	public String getName() {
		return name;
	}
	
	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
	
	public String getTable() {
		return table;
	}

	public String getRefTable() {
		return refTable;
	}
	
	public String getColumn() {
		return column;
	}

	public String getRefColumn() {
		return refColumn;
	}

	@Override
	public String toString() {
		return "ForeignKeyStatus [name=" + name + ", table=" + table
				+ ", column=" + column + ", refTable=" + refTable
				+ ", refColumn=" + refColumn + ", type=" + type + "]";
	}

	
}
