package com.ssrg.r2c.coloriented;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

public class Table implements Cloneable {

	private String name;
	private String alias;
	private List<RowkeyColumn> key;
	private Hashtable<String, ColumnFamily> colFamilies;
	private String baseRelationalTable;
	private boolean deleted;

	public Table(String tableName) {
		this.name = tableName;
		this.alias = tableName;
		this.deleted = false;

		key = new ArrayList<RowkeyColumn>();
		colFamilies = new Hashtable<String, ColumnFamily>();
		colFamilies.put(tableName, new ColumnFamily(tableName, tableName,
				ColumnFamily.Type.ONE));

		baseRelationalTable = tableName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getBaseRelationalTableName() {
		return baseRelationalTable;
	}

	public void setBaseRelationalTableName(String name) {
		this.baseRelationalTable = name;
	}

	public List<RowkeyColumn> getKey() {
		return key;
	}

	public void setKey(List<RowkeyColumn> columns) {
		key = columns;
	}
	
	public boolean isSubsetOfKey(List<String> list) {
		for (String listColName : list) {
			boolean found = false;
			
			for (RowkeyColumn keyCol : getKey()) {
				if (keyCol.getName().equals(listColName)) {
					found = true;
				}
			}
			
			if (!found) {
				return false;
			}
		}
		
		return true;
	}

	public void setAndEncodeKey(List<Column> columns) {
		List<RowkeyColumn> keyColumns = new ArrayList<RowkeyColumn>();
		int columnsLength = columns.size();

		for (int i = 0; i < columnsLength; i++) {
			Column column = columns.get(i);

			if (column.isGeo() && column.isLatitude()) {

				for (int j = i + 1; j < columnsLength; j++) {
					Column colAdv = columns.get(j);

					if (colAdv.isGeo() && colAdv.isLongitude()) {
						RowkeySpatialColumn spatialCol = new RowkeySpatialColumn(
								column, column, colAdv);
						keyColumns.add(spatialCol);
						columns.remove(j);
						columnsLength--;
						break;
					}

					// if it doesn't find the matching coordinate, just add it
					// as it is to the rowkey
					keyColumns.add(new RowkeyColumn(column));
				}
			} else if (column.isGeo() && column.isLatitude()) {

				for (int j = i + 1; j < columnsLength; j++) {
					Column colAdv = columns.get(j);

					if (colAdv.isGeo() && colAdv.isLongitude()) {
						RowkeySpatialColumn spatialCol = new RowkeySpatialColumn(
								column, column, colAdv);
						keyColumns.add(spatialCol);
						columns.remove(j);
						columnsLength--;
						break;
					}

					// if it doesn't find the matching coordinate, just add
					// it as it is to the rowkey
					keyColumns.add(new RowkeyColumn(column));
				}
			} else if (column.isTemporal()) {
				keyColumns.add(new RowkeyColumn(column,
						RowkeyColumn.Encoding.TIMESTAMP));
			} else {
				keyColumns.add(new RowkeyColumn(column));
			}
		}

		key = keyColumns;
	}

	public boolean containsColumnFamily(String name) {
		if (colFamilies.containsKey(name)) {
			return true;
		}

		return false;
	}

	public Column getColumn(String name) {
		ColumnFamily colFamily = colFamilies.get(this
				.getBaseRelationalTableName());

		for (Column col : colFamily.getColumns()) {
			if (col.getName().equals(name)) {
				return col;
			}
		}

		return null;
	}

	public Collection<ColumnFamily> getColumnFamilies() {
		return colFamilies.values();
	}

	public void addColumnFamily(String name, String base, ColumnFamily.Type type) {
		ColumnFamily c = new ColumnFamily(name, base, type);
		colFamilies.put(name, c);
	}

	public void addColumnFamily(ColumnFamily c) {
		colFamilies.put(c.getName(), c);
	}

	public ColumnFamily getColumnFamily(String name) {
		if (colFamilies.containsKey(name)) {
			return colFamilies.get(name);
		}
		System.out.println("NULL");
		System.out.println(colFamilies.keySet());
		return null;
	}

	public void addColumn(String family, String colTable, String colName,
			String type, boolean suggest) {
		if (colFamilies.containsKey(family)) {
			colFamilies.get(family).addColumn(
					new Column(colTable, colName, type, suggest));
		}
	}

	public void lazyDelete() {
		this.deleted = true;
	}

	public boolean isDeleted() {
		return this.deleted;
	}

	public Table cloneTable() {
		try {
			return (Table) this.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		Table clone = new Table(this.name);

		for (ColumnFamily colFamily : colFamilies.values()) {
			clone.addColumnFamily((ColumnFamily) colFamily.clone());
		}

		return clone;
	}

	@Override
	public String toString() {

		String s = name + "\n";
		s += "\tkey: " + key + "\n";
		s += "\tdeleted: " + deleted + "\n";
		s += "\talias: " + alias + "\n";

		for (Entry<String, ColumnFamily> fam : colFamilies.entrySet()) {
			ColumnFamily f = fam.getValue();

			s += "\n\tname: " + f.getName() + "\n";
			s += "\tbase: " + f.getBaseRelationalTable() + "\n";
			s += "\ttype: " + f.getType() + "\n";
			s += "\tSQL: " + f.getSql() + "\n";

			if (f.getType() == ColumnFamily.Type.MANY) {
				s += "\tMANY: " + f.getManyKey() + "\n";
			}

			for (Column col : f.getColumns()) {
				s += "\t\t";
				if (col.isChecked()) {
					s += "[X] ";
				} else {
					s += "[ ] ";
				}

				s += col.getName() + "\n";
			}
		}

		return s;
	}

}
