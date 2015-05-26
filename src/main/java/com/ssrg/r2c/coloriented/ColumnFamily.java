package com.ssrg.r2c.coloriented;

import java.util.ArrayList;
import java.util.List;

public class ColumnFamily implements Cloneable {

	private String name;
	private String alias;
	private String baseRelationalTable;
	private List<Column> columns;
	private Type type;
	private List<Column> manyKey;

	private String sql;

	public enum Type {
		ONE, MANY, EXT
	}

	public ColumnFamily(String name, String base, Type type) {
		this.name = name;
		this.alias = name;
		this.baseRelationalTable = base;
		this.type = type;
		columns = new ArrayList<Column>();
	}

	public String getBaseRelationalTable() {
		return baseRelationalTable;
	}

	public String getSql() {
		return sql;
	}
	
	public Type getType() {
		return type;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public String getName() {
		return name;
	}

	public List<Column> getColumns() {
		return columns;
	}
	
	public Column getColumn(String colName) {
		for (Column c : columns) {
			if (c.getName().equals(colName)) {
				return c;
			}
		}
		
		return null;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public void addColumn(Column column) {
		this.columns.add(column);
	}
	
	public void setManyKey(List<Column> manyKey) {
		this.manyKey = manyKey;
	}
	
	public List<Column> getManyKey() {
		return manyKey;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		ColumnFamily clone = new ColumnFamily(name, baseRelationalTable, type);
		clone.manyKey = manyKey;
		
		for (Column col : columns) {
			clone.addColumn((Column) col.clone());
		}
		
		return clone;
	}

	@Override
	public String toString() {
		return "ColumnFamily [name=" + name + ", columns=" + columns + "]";
	}
}
