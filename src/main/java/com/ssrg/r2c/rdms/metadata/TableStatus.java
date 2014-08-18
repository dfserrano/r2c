package com.ssrg.r2c.rdms.metadata;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableStatus {

	private String name;
	private int rowCount;
	private Date creationDate;
	private Date lastUpdate;

	private Type type;
	private Strength strength;

	public enum Type {
		ACTIVE, PASSIVE
	};

	public enum Strength {
		WEAK, STRONG
	};

	private List<IndexStatus> indexes;
	private List<ForeignKeyStatus> references;
	private List<ColumnStatus> columns;

	private List<IndexStatus> primaryKey;

	public TableStatus(String name, int row, Date creation, Date update,
			List<IndexStatus> indexes,
			List<ForeignKeyStatus> references,
			List<ColumnStatus> columns) throws Exception {
		this.name = name;
		rowCount = row;
		creationDate = creation;
		lastUpdate = update;

		this.indexes = indexes;
		this.references = references;
		this.columns = columns;
		
		removeIndexesForForeignKeys();
		findPrimaryKey();
	}

	public String getName() {
		return name;
	}

	public List<ColumnStatus> getColumns() {
		return this.columns;
	}

	public float getInsertionRate() {
		return this.rowCount / differenceInDays(this.creationDate);
	}

	public static int differenceInDays(Date from) {
		long MILLISECONDS_IN_DAY = 24 * 60 * 60 * 1000;
		Date now = new Date(Calendar.getInstance().getTimeInMillis());

		return (int) ((now.getTime() - from.getTime()) / MILLISECONDS_IN_DAY);
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public Strength getStrength() {
		return strength;
	}

	public void setStrength(Strength strength) {
		this.strength = strength;
	}

	public List<IndexStatus> getIndexes() {
		return indexes;
	}

	public List<ForeignKeyStatus> getReferences() {
		return references;
	}

	private boolean existReferenceName(String name) {
		for (ForeignKeyStatus fk : references) {
			if (fk.getName().equals(name)) {
				return true;
			}
		}

		return false;
	}

	public void removeIndexesForForeignKeys() {
		for (IndexStatus index : new ArrayList<IndexStatus>(indexes)) {
			if (existReferenceName(index.getName())) {
				indexes.remove(index);
			}
		}
	}

	public IndexStatus getUniqueIndexFromColumn(String columnName) {
		for (IndexStatus index : indexes) {
			if (index.getColumn().equals(columnName) && index.isUnique()) {
				return index;
			}
		}

		return null;
	}

	public List<IndexStatus> getPrimaryKey() {
		return primaryKey;
	}
	
	private void findPrimaryKey() throws Exception {
		Set<ColumnStatus> keyColumns = new HashSet<ColumnStatus>();

		for (ColumnStatus col : columns) {
			if (col.isPrimaryKey()) {
				keyColumns.add(col);
			}
		}

		if (keyColumns.size() > 0) {
			Map<String, List<IndexStatus>> indexesGrouped = getIndexesGrouped();

			for (List<IndexStatus> indexGroup : indexesGrouped.values()) {
				if (indexGroup.size() == keyColumns.size()) {

					int count = 0;
					for (ColumnStatus col : keyColumns) {
						boolean found = false;

						for (IndexStatus col2 : indexGroup) {
							if (col.getName().equals(col2.getColumn())) {
								found = true;
								count++;
								break;
							}
						}

						if (!found) {
							break;
						}
					}

					if (indexGroup.size() == count) {
						primaryKey = indexGroup;
					}
				}
			}
		}

		if (primaryKey == null) {
			throw new Exception("Table does not have a primary key");
		}
	}

	public Map<String, List<IndexStatus>> getIndexesGrouped() {
		Map<String, List<IndexStatus>> indexesGrouped = new HashMap<String, List<IndexStatus>>();

		for (IndexStatus index : indexes) {

			if (!indexesGrouped.containsKey(index.getName())) {
				indexesGrouped.put(index.getName(),
						new ArrayList<IndexStatus>());
			}

			List<IndexStatus> curSet = indexesGrouped.get(index.getName());
			curSet.add(index);
			indexesGrouped.put(index.getName(), curSet);
		}

		return indexesGrouped;
	}

	@Override
	public String toString() {
		return "\n\tTableStatus [name=" + name + ", rowCount=" + rowCount
				+ ", creationDate=" + creationDate + ", lastUpdate="
				+ lastUpdate + ", referencesCount=" + references.size() + "]\n";
	}
}
