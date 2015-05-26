package com.ssrg.r2c.coloriented;

import java.util.Collection;
import java.util.Hashtable;

public class Schema {

	private Hashtable<String, Table> tables;

	public Schema() {
		tables = new Hashtable<String, Table>();
	}

	public void addTable(String name, Table t) {
		tables.put(name, t);
	}

	public Table getTable(String name) {
		return tables.get(name);
	}

	public Table getTable(String relationalName, String indexName) {
		for (Table table : tables.values()) {
			if (table.getBaseRelationalTableName().equals(relationalName)
					&& table.getIndexName().equals(indexName)) {
				return table;
			}
		}
		
		return null;
	}

	public Collection<Table> getTables() {
		return tables.values();
	}

	public void removeTable(String name) {
		tables.remove(name);
	}

	@Override
	public String toString() {
		return "Schema [tables=" + tables + "]";
	}
}
