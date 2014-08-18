package com.ssrg.r2c.rdms.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

public class DatabaseStatus {

	private String name;
	private Hashtable<String, TableStatus> tables;
	private float avgInsertRate = 0.0f;
	private float[] quartilesInsertRate = new float[3];

	public enum RelationshipCardinality {
		ONETOONE, ONETOMANY, MANYTOONE
	};

	public DatabaseStatus(String name) {
		this.name = name;
	}

	public Collection<TableStatus> getTables() {
		return tables.values();
	}

	public Set<String> getTableNames() {
		return tables.keySet();
	}

	public void setTables(Hashtable<String, TableStatus> tables) {
		this.tables = tables;
	}

	public TableStatus getTable(String tableName) {
		return tables.get(tableName);
	}

	public List<IndexStatus> getIndexes(String tableName) {
		return tables.get(tableName).getIndexes();
	}

	public List<String> getTableColumnNamesFromIndex(String tableName,
			String indexName) {
		List<String> columnNames = new ArrayList<String>();

		List<IndexStatus> indexes = getIndexes(tableName);

		for (IndexStatus index : indexes) {
			if (index.getName().equals(indexName)) {
				columnNames.add(index.getColumn());
			}
		}

		return columnNames;
	}

	public List<ForeignKeyStatus> getTableReferences(String tableName)
			throws Exception {
		if (tables.get(tableName) != null) {
			return tables.get(tableName).getReferences();
		}

		throw new Exception("Table does not exist");
	}

	public boolean areTablesConnected(String tableNameA, String tableNameB)
			throws Exception {
		if (areTablesConnectedInOneToMany(tableNameA, tableNameB)
				|| areTablesConnectedInOneToMany(tableNameB, tableNameA)) {
			return true;
		}

		return false;
	}

	public List<ForeignKeyStatus> getForeignKey(String foreignKeyName) {
		List<ForeignKeyStatus> result = new ArrayList<ForeignKeyStatus>();
		
		for (TableStatus table : tables.values()) {
			for (ForeignKeyStatus fk : table.getReferences()) {
				if (fk.getName().equals(foreignKeyName)) {
					result.add(fk);
				}
			}
		}
		
		return result;
	}

	public List<ForeignKeyStatus> getForeignKeysConnectingTables(
			String tableNameA, String tableNameB) throws Exception {
		List<ForeignKeyStatus> result = getForeignKeysConnectedInOneToMany(
				tableNameA, tableNameB);
		result.addAll(getForeignKeysConnectedInOneToMany(tableNameB, tableNameA));

		return result;
	}

	public List<ForeignKeyStatus> getForeignKeysConnectedInOneToMany(
			String tableNameOne, String tableNameMany) throws Exception {
		List<ForeignKeyStatus> result = new ArrayList<ForeignKeyStatus>();
		List<ForeignKeyStatus> refs = getTableReferences(tableNameOne);

		for (ForeignKeyStatus ref : refs) {
			if (ref.getRefTable().equals(tableNameMany)) {
				result.add(ref);
			}
		}

		return result;
	}

	public boolean areTablesConnectedInOneToMany(String tableNameOne,
			String tableNameMany) throws Exception {
		List<ForeignKeyStatus> refs = getTableReferences(tableNameOne);

		for (ForeignKeyStatus ref : refs) {
			if (ref.getRefTable().equals(tableNameMany)) {
				return true;
			}
		}

		return false;
	}

	public boolean isTableReferencedByOthers(String tableName) throws Exception {
		for (TableStatus t : tables.values()) {
			List<ForeignKeyStatus> refs = getTableReferences(t.getName());

			for (ForeignKeyStatus ref : refs) {
				if (ref.getRefTable().equals(tableName)) {
					return true;
				}
			}
		}

		return false;
	}

	public List<ForeignKeyStatus> getReferencesBetweenTables(String tableNameA,
			String tableNameB) throws Exception {
		List<ForeignKeyStatus> references = new ArrayList<ForeignKeyStatus>();

		List<ForeignKeyStatus> refsA = getTableReferences(tableNameA);

		for (ForeignKeyStatus ref : refsA) {
			if (ref.getRefTable().equals(tableNameB)) {
				references.add(ref);
			}
		}

		List<ForeignKeyStatus> refsB = getTableReferences(tableNameB);

		for (ForeignKeyStatus ref : refsB) {
			if (ref.getRefTable().equals(tableNameA)) {
				references.add(ref);
			}
		}

		return references;
	}

	public RelationshipCardinality getRelationshipCardinality(
			String tableNameA, String tableNameB) throws Exception {
		List<ForeignKeyStatus> refsA = getTableReferences(tableNameA);

		for (ForeignKeyStatus ref : refsA) {
			if (ref.getRefTable().equals(tableNameB)) {
				if (ref.getType().equals(ForeignKeyStatus.Type.ONE)) {
					return RelationshipCardinality.ONETOONE;
				} else {
					return RelationshipCardinality.MANYTOONE;
				}
			}
		}

		List<ForeignKeyStatus> refsB = getTableReferences(tableNameB);

		for (ForeignKeyStatus ref : refsB) {
			if (ref.getType().equals(ForeignKeyStatus.Type.ONE)) {
				return RelationshipCardinality.ONETOONE;
			} else {
				return RelationshipCardinality.ONETOMANY;
			}
		}

		return null;
	}

	public float getAvgInsertRate() {
		return avgInsertRate;
	}

	public void setAvgInsertRate(float avgInsertRate) {
		this.avgInsertRate = avgInsertRate;
	}

	public float getQuartileInsertRate(int numQuartile) {
		return quartilesInsertRate[numQuartile - 1];
	}

	public void setQuartileInsertRate(int numQuartile, float qInsertRate) {
		this.quartilesInsertRate[numQuartile - 1] = qInsertRate;
	}

	public String getName() {
		return name;
	}

	public boolean isUniqueKey(String tableName, String columnName) {
		if (getUniqueKeyName(tableName, columnName) != null) {
			return true;
		}

		return false;
	}

	public String getUniqueKeyName(String tableName, String columnName) {
		TableStatus ts = tables.get(tableName);
		IndexStatus index = ts.getUniqueIndexFromColumn(columnName);

		if (index != null) {
			return index.getName();
		}

		return null;
	}

	@Override
	public String toString() {
		return "DatabaseStatus [name=" + name + ", tables=" + tables
				+ ", avgInsertRate=" + avgInsertRate + ", quartilesInsertRate="
				+ Arrays.toString(quartilesInsertRate) + "]";
	}
}
