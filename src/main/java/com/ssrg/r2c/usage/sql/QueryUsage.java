package com.ssrg.r2c.usage.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ssrg.r2c.rdms.metadata.DatabaseStatus;
import com.ssrg.r2c.rdms.metadata.ForeignKeyStatus;
import com.ssrg.r2c.rdms.metadata.IndexStatus;
import com.ssrg.r2c.rdms.metadata.TableStatus;
import com.ssrg.r2c.usage.FileLoader;
import com.ssrg.r2c.usage.TablePairActivity;

public class QueryUsage {

	private DatabaseStatus db;
	private List<Query> queries;

	public QueryUsage(DatabaseStatus db) {
		this.queries = new ArrayList<Query>();
		this.db = db;
	}

	public QueryUsage(DatabaseStatus db, String queryLogPath) {
		this(db);

		this.loadTestUsage(queryLogPath);
	}

	public TablePairActivity getIndexUsageForTablePair(
			TablePairActivity tablePair) throws Exception {

		findIndexUsageForPair(tablePair);
		findIndexUsageForSingle(tablePair);
		findIndexUsageInVicinity(tablePair);

		return tablePair;
	}

	public boolean isEmpty() {
		if (queries != null && queries.size() > 0) {
			return false;
		}
		return true;
	}

	public void findIndexUsageForPair(TablePairActivity tablePair) {
		for (Query query : queries) {
			List<String> queryRelations = query.getRelations();
			Set<QueryFilter> queryFilters = query.getSelections();

			String tableA = tablePair.getNameOfTableA();
			String tableB = tablePair.getNameOfTableB();

			if (tablePair.isPair() && queryRelations.contains(tableA)
					&& queryRelations.contains(tableB)) {
				// check queries that contain both of the tables
				for (QueryFilter qf : queryFilters) {
					QueryAttribute qa = qf.getLeftAttribute();

					// if the query uses a unique index, then that table is
					// credited with the number of times the query appears
					if (db.isUniqueKey(qa.getTableName(), qa.getColumnName())) {
						if (qa.getTableName().equals(tableA)) {
							tablePair.addUsageIndexA(query.getTimesUsed());
						} else if (qa.getTableName().equals(tableB)) {
							tablePair.addUsageIndexB(query.getTimesUsed());
						}
					}
				}
			}
		}
	}

	private void findIndexUsageForSingle(TablePairActivity tablePair) {
		for (Query query : queries) {
			List<String> queryRelations = query.getRelations();
			Set<QueryFilter> queryFilters = query.getSelections();

			String tableA = tablePair.getNameOfTableA();

			if (tablePair.isSingle() && queryRelations.contains(tableA)) {
				// Check queries that use that table
				if (queryRelations.size() == 1) {
					// if the table is used in a query as the only table,
					// then account as index usage to prevent deletion
					tablePair.addUsageIndexA(query.getTimesUsed());
				} else {
					// if the table is used in a query, in conjunction with
					// other tables, and its index is used
					for (QueryFilter qf : queryFilters) {
						QueryAttribute qa = qf.getLeftAttribute();

						// TODO: unique or just key?
						if (db.isUniqueKey(qa.getTableName(),
								qa.getColumnName())) {
							if (qa.getTableName().equals(tableA)) {
								tablePair.addUsageIndexA(query.getTimesUsed());
							}
						}
					}
				}
			}
		}
	}

	private void findIndexUsageInVicinity(TablePairActivity tablePair)
			throws Exception {
		for (Query query : queries) {
			List<String> queryRelations = query.getRelations();
			Set<QueryFilter> queryFilters = query.getSelections();

			String tableA = tablePair.getNameOfTableA();
			String tableB = tablePair.getNameOfTableB();

			// For complex queries with more than 2 tables, and where we haven't
			// found index usage
			if (tablePair.isPair() && tablePair.getUsageIndexA() == 0
					&& tablePair.getUsageIndexB() == 0
					&& queryRelations.size() > 2) {
				for (QueryFilter qa : queryFilters) {
					String targetTable = qa.getLeftAttribute().getTableName();

					if (db.areTablesConnectedInOneToMany(tableA, targetTable)) {
						tablePair.addUsageNearIndexA(query.getTimesUsed());
					} else if (db.areTablesConnectedInOneToMany(tableB,
							targetTable)) {
						tablePair.addUsageNearIndexB(query.getTimesUsed());
					}
				}
			}
		}
	}

	public Set<TablePairActivity> extractTablePairsFromQueries()
			throws Exception {
		Set<TablePairActivity> tablePairs = new HashSet<TablePairActivity>();

		for (Query q : queries) {
			List<String> relations = q.getRelations();

			if (relations.size() > 1) {

				for (int i = 0; i < relations.size(); i++) {
					String relation1 = relations.get(i);

					for (int j = i + 1; j < relations.size(); j++) {
						String relation2 = relations.get(j);
						
						List<ForeignKeyStatus> connections = db
								.getForeignKeysConnectingTables(relation1,
										relation2);
						if (connections.size() > 0) {
							for (ForeignKeyStatus connection : connections) {
								TablePairActivity tpa = new TablePairActivity(
										connection.getName(), relation1,
										relation2);
								tablePairs.add(tpa);
							}

						}
					}
				}
			}
		}

		return tablePairs;
	}

	public Set<TablePairActivity> extractTablePairsFromSchema() {
		Set<TablePairActivity> tablePairs = new HashSet<TablePairActivity>();

		for (TableStatus t : db.getTables()) {

			for (ForeignKeyStatus fk : t.getReferences()) {
				TablePairActivity tpa = new TablePairActivity(fk.getName(),
						fk.getTable(), fk.getRefTable());
				tablePairs.add(tpa);
			}
		}

		return tablePairs;
	}

	public boolean areTablesUsedInTheSameQuery(String tableNameA,
			String tableNameB) {
		for (Query q : queries) {
			List<String> relations = q.getRelations();

			if (relations.size() > 1) {
				if (relations.contains(tableNameA)
						&& relations.contains(tableNameB)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean isColumnUsedInTheSameQueryWithTable(String tableA,
			String columnA, String tableB) {
		for (Query q : queries) {
			List<String> relations = q.getRelations();

			if (relations.size() > 1) {
				if (relations.contains(tableA) && relations.contains(tableB)) {
					List<QueryAttribute> projections = q.getProjections();

					for (QueryAttribute qa : projections) {
						if (qa.getTableName().equals(tableA)
								&& qa.getColumnName().equals(columnA)) {
							return true;
						}
					}

				}
			}
		}

		return false;
	}

	private HashMap<String, Long> getKeyColumnsFromQueries(String tableName) {
		HashMap<String, Long> indexUsageCount = new HashMap<String, Long>();
		Set<String> usedKeys;

		for (Query query : queries) {
			Set<QueryFilter> queryFilters = query.getSelections();
			usedKeys = new HashSet<String>();

			for (QueryFilter qf : queryFilters) {
				QueryAttribute qa = qf.getLeftAttribute();

				if (qa.getTableName().equals(tableName)) {
					String keyName = db.getUniqueKeyName(qa.getTableName(),
							qa.getColumnName());

					if (keyName != null && !usedKeys.contains(keyName)) {
						long count = 0;
						if (indexUsageCount.containsKey(keyName)) {
							count = indexUsageCount.get(keyName);
						}
						indexUsageCount.put(keyName,
								count + query.getTimesUsed());

						usedKeys.add(keyName);
					}
				}
			}
		}

		return indexUsageCount;
	}

	public List<String> getKeyColumns(String tableName) {
		List<String> keyColumns = new ArrayList<String>();
		HashMap<String, Long> indexUsageCount = getKeyColumnsFromQueries(tableName);

		if (indexUsageCount.size() > 0) {
			// find most used
			long maxCount = 0;
			String maxName = null;

			for (String name : indexUsageCount.keySet()) {
				long val = indexUsageCount.get(name);

				if (val > maxCount) {
					maxName = name;
				}
			}

			if (maxName != null) {
				// find columns of key
				keyColumns.addAll(db.getTableColumnNamesFromIndex(tableName,
						maxName));
			}

		}

		// By default, add primary key
		if (keyColumns.size() == 0) {
			// add primary key
			List<IndexStatus> pk = db.getTable(tableName).getPrimaryKey();
			for (IndexStatus index : pk) {
				keyColumns.add(index.getColumn());
			}
		}

		return keyColumns;
	}

	private void loadTestUsage(String queryLogPath) {
		queries = FileLoader.loadQueriesFromFile(queryLogPath);
	}

}
