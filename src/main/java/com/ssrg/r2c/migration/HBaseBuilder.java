package com.ssrg.r2c.migration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.ssrg.r2c.Configuration;
import com.ssrg.r2c.Main;
import com.ssrg.r2c.coloriented.Column;
import com.ssrg.r2c.coloriented.ColumnFamily;
import com.ssrg.r2c.coloriented.RowkeyColumn;
import com.ssrg.r2c.coloriented.RowkeySpatialColumn;
import com.ssrg.r2c.coloriented.Schema;
import com.ssrg.r2c.coloriented.Table;
import com.ssrg.r2c.rdms.metadata.ColumnStatus;
import com.ssrg.r2c.rdms.metadata.DatabaseStatus;
import com.ssrg.r2c.rdms.metadata.ForeignKeyStatus;
import com.ssrg.r2c.rdms.metadata.IndexStatus;
import com.ssrg.r2c.rdms.metadata.TableStatus;
import com.ssrg.r2c.usage.TablePairActivity;
import com.ssrg.r2c.usage.sql.Query;
import com.ssrg.r2c.usage.sql.QueryAttribute;
import com.ssrg.r2c.usage.sql.QueryFilter;
import com.ssrg.r2c.usage.sql.QueryUsage;

public class HBaseBuilder implements ColumnOrientedBuilder {

	// product
	private Schema schema;
	private DatabaseStatus db;
	private QueryUsage usage;
	private Configuration conf;

	static final Logger logger = Logger.getLogger(HBaseBuilder.class);

	public HBaseBuilder(DatabaseStatus db, QueryUsage usage, Configuration conf)
			throws Exception {
		BasicConfigurator.configure();
		logger.debug("Constructing instance of HBaseBuilder");

		if (db == null) {
			logger.debug("DatabaseStatus is null.  Throwing Exception");
			throw new Exception("Database not ready for mapping");
		}

		if (usage == null) {
			logger.debug("QueryUsage is null.  Throwing Exception");
			throw new Exception("Usage not ready for mapping");
		}

		this.db = db;
		this.usage = usage;
		this.conf = conf;
	}

	public Schema getSchema() {
		return schema;
	}

	public void initializeSchema() {
		// Initialize target schema with the same tables as relational db
		System.out.println("Initializing HBase schema...");
		logger.info("Initializing HBase schema");

		schema = new Schema();
		for (String tableName : db.getTableNames()) {
			Table table = new Table(tableName);
			table.setIndexName(db.getTable(tableName).getPrimaryKeyName());
			schema.addTable(tableName, table);
		}
	}

	public void mergeOneToOneRelationships() {
		System.out.println("Merging 1-1 Relationships...");
		logger.info("Merging 1-1 Relationships");

		for (TableStatus ts : db.getTables()) {
			String tableName = ts.getName();
			List<ForeignKeyStatus> fkList = ts.getReferences();

			for (ForeignKeyStatus fkStatus : fkList) {
				if (fkStatus.getType() == ForeignKeyStatus.Type.ONE) {
					// The target is the referenced table
					Table target = schema.getTable(fkStatus.getRefTable());

					target.addColumnFamily(fkStatus.getName(),
							fkStatus.getTable(), ColumnFamily.Type.ONE);

					try {
						if (db.isTableReferencedByOthers(tableName)) {
							// if the table is referenced by others, then don't
							// delete
							Table table = schema.getTable(tableName);

							table.addColumnFamily(fkStatus.getName(),
									fkStatus.getRefTable(),
									ColumnFamily.Type.ONE);
						} else {
							// Removes the table that references the main table
							schema.getTable(fkStatus.getTable()).lazyDelete();
						}
					} catch (Exception e) {
						e.printStackTrace();
						logger.error(tableName + " is not a valid table");
					}
				}
			}
		}
	}

	public void mergeOneToManyRelationships() {
		System.out.println("Merging 1-N Relationships...");
		logger.info("Merging 1-N Relationships");

		// Extract co-occurrences of tables in queries
		Set<TablePairActivity> tablePairs = null;
		if (!usage.isEmpty()) {
			try {
				tablePairs = usage.extractTablePairsFromQueries();
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Error trying to extract table pairs from queries");
			}
		} else {
			// when there is no log information, use pairs formed by schema
			tablePairs = usage.extractTablePairsFromSchema();
		}

		for (TablePairActivity tablePair : tablePairs) {
			try {
				// Updates table pair with information about usage of the two
				// tables in queries
				usage.getIndexUsageForTablePair(tablePair);

				// Merges B into A
				if (tablePair.getUsageIndexA() >= conf.getUsageThreshold()
						|| tablePair.getUsageNearIndexA() >= conf
								.getUsageThreshold() || usage.isEmpty()) {
					combineOneToMany(schema, tablePair.getRelationName(),
							tablePair.getNameOfTableA(),
							tablePair.getNameOfTableB(), db);
				}

				// Merges A into B
				if (tablePair.getUsageIndexB() >= conf.getUsageThreshold()
						|| tablePair.getUsageNearIndexB() >= conf
								.getUsageThreshold() || usage.isEmpty()) {
					combineOneToMany(schema, tablePair.getRelationName(),
							tablePair.getNameOfTableB(),
							tablePair.getNameOfTableA(), db);
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Error trying to combine 1-N relationships");
			}
		}
	}

	private void combineOneToMany(Schema schema, String relationName,
			String toTable, String fromTable, DatabaseStatus ds)
			throws Exception {
		Table target = schema.getTable(toTable);

		DatabaseStatus.RelationshipCardinality type = ds
				.getRelationshipCardinality(toTable, fromTable);
		if (type == DatabaseStatus.RelationshipCardinality.ONETOMANY) {
			target.addColumnFamily(relationName, fromTable,
					ColumnFamily.Type.MANY);
		} else if (type == DatabaseStatus.RelationshipCardinality.MANYTOONE) {
			target.addColumnFamily(relationName, fromTable,
					ColumnFamily.Type.ONE);
		}

		TablePairActivity singleTable = new TablePairActivity(fromTable);
		singleTable = usage.getIndexUsageForTablePair(singleTable);

		if (singleTable.getUsageIndexA() < conf.getUsageThreshold()
				&& !usage.isEmpty()) {
			if (schema.getTable(fromTable) != null) {
				schema.getTable(fromTable).lazyDelete();
			}
		}
	}

	public void addColumnsToCurrentSchema() {
		System.out.println("Adding columns for current schema...");
		logger.info("Adding columns for current schema");

		for (Table colOrientedTable : schema.getTables()) {
			String tableName = colOrientedTable.getName();

			for (ColumnFamily columnFamily : colOrientedTable
					.getColumnFamilies()) {

				List<ColumnStatus> tableCols = db.getTable(
						columnFamily.getBaseRelationalTable()).getColumns();

				for (ColumnStatus colStatus : tableCols) {
					boolean checked = false;

					if (tableName.equals(columnFamily.getBaseRelationalTable())
							|| colStatus.isPrimaryKey()
							|| usage.isColumnUsedInTheSameQueryWithTable(
									columnFamily.getBaseRelationalTable(),
									colStatus.getName(), tableName)) {
						checked = true;
					}

					if (Main.ENV.equals("DEBUG")) {
						checked = true;
					}

					if (columnFamily.getBaseRelationalTable().equals(
							"customers")
							&& !tableName.equals("customers")
							&& !(colStatus.getName().equals("LASTNAME")
									|| colStatus.getName().equals("FIRSTNAME") || colStatus
									.getName().equals("CUSTOMERID"))) {
						checked = false;
					}
					
					if (columnFamily.getBaseRelationalTable().equals(
							"products")
							&& !tableName.equals("products")
							&& !(colStatus.getName().equals("PROD_ID")
									|| colStatus.getName().equals("TITLE"))) {
						checked = false;
					}

					colOrientedTable.addColumn(columnFamily.getName(),
							columnFamily.getBaseRelationalTable(),
							colStatus.getName(), colStatus.getType(), checked);
				}
			}
		}

	}

	public void extendSchema() {
		// Recursive method to add 1-1 Relationships from vicinity up to a
		// defined level
		System.out.println("Extending schema...");
		logger.info("Extending schema");

		for (Table table : schema.getTables()) {
			Set<ColumnFamily> extendedColumnFamilies = extendAndMerge(table, 1,
					"", table);

			for (ColumnFamily columnFamily : extendedColumnFamilies) {

				// Nested families are added as a special type EXT
				table.addColumnFamily(columnFamily.getName(),
						columnFamily.getBaseRelationalTable(),
						ColumnFamily.Type.EXT);
				table.getColumnFamily(columnFamily.getName()).setManyKey(columnFamily.getManyKey());
				
				String[] pieces = columnFamily.getBaseRelationalTable().split(
						conf.getExtColFamilyDelimiter());
				String tableName = pieces[pieces.length - 1];

				for (Column col : columnFamily.getColumns()) {
					boolean checked = false;

					if (usage.isColumnUsedInTheSameQueryWithTable(tableName,
							col.getName(), table.getName())) {
						checked = true;
					}

					if (Main.ENV.equals("DEBUG")) {
						checked = true;
					}
					
					if (columnFamily.getBaseRelationalTable().equals(
							"products")
							&& !table.getName().equals("products")
							&& !(col.getName().equals("PROD_ID")
									|| col.getName().equals("TITLE"))) {
						checked = false;
					}
					
					

					table.addColumn(columnFamily.getName(), col.getTable(),
							col.getName(), col.getType(), checked);
				}
			}
		}
	}

	private Set<ColumnFamily> extendAndMerge(Table table, int currentLevel,
			String prefix, Table rootTable) {

		if (currentLevel > conf.getMaxExtLevel()) {
			return null;
		}

		Set<ColumnFamily> result = new HashSet<ColumnFamily>();

		for (ColumnFamily colFamily : table.getColumnFamilies()) {

			Table relatedTable = schema.getTable(colFamily
					.getBaseRelationalTable());

			if (relatedTable != null
					&& (!table.getName().equals(relatedTable.getName()))) {
				for (ColumnFamily relatedColFamily : relatedTable
						.getColumnFamilies()) {

					try {
						// Don't consider 1-1 relationships as they are already
						// merged by this time
						if (db.getRelationshipCardinality(
								table.getBaseRelationalTableName(),
								relatedTable.getBaseRelationalTableName()) != DatabaseStatus.RelationshipCardinality.ONETOONE) {
							// Only consider extending through ONE type
							// connections
							// MANY families are only considered if it is in
							// first level
							if (relatedColFamily.getType() == ColumnFamily.Type.ONE
									&& (!colFamily.getName().equals(
											relatedColFamily.getName()))
									&& (currentLevel == 1 || colFamily
											.getType() == ColumnFamily.Type.ONE)
									&& !rootTable
											.containsColumnFamily(relatedColFamily
													.getName())) {

								// don't add main column families
								if (relatedColFamily.getBaseRelationalTable()
										.equals(relatedColFamily.getName())) {
									continue;
								}

								// Remove leading hyphen from prefix delimiter
								// (-)
								String newColFamilyName = (prefix.isEmpty()) ? colFamily
										.getName()
										+ conf.getExtColFamilyDelimiter()
										+ relatedColFamily.getName()
										: prefix
												+ conf.getExtColFamilyDelimiter()
												+ colFamily.getName()
												+ conf.getExtColFamilyDelimiter()
												+ relatedColFamily.getName();

								ColumnFamily newColumnFamily = new ColumnFamily(
										newColFamilyName,
										relatedColFamily
												.getBaseRelationalTable(),
										ColumnFamily.Type.EXT);
								
								// pass the key for nested entity
								newColumnFamily.setManyKey(colFamily.getManyKey());

								for (Column column : relatedColFamily
										.getColumns()) {
									boolean checked = false;

									/*if (Main.ENV.equals("DEBUG")) {
										checked = true;
									}
									
									System.out.println("\t" + relatedColFamily.getBaseRelationalTable());
									System.out.println("\t\t" + table.getName());
									System.out.println("\t\t\t" + column.getName());
									if (relatedColFamily.getBaseRelationalTable().equals(
											"products")
											&& !table.getName().equals("products")
											&& !(column.getName().equals("PROD_ID")
													|| column.getName().equals("TITLE"))) {
										checked = false;
									}*/

									Column newColumn = new Column(
											column.getTable(),
											column.getName(), column.getType(),
											checked);
									newColumnFamily.addColumn(newColumn);
								}

								result.add(newColumnFamily);

								String newPrefix = (prefix.isEmpty()) ? colFamily
										.getName() : prefix
										+ conf.getExtColFamilyDelimiter()
										+ colFamily.getName();

								Set<ColumnFamily> r = extendAndMerge(
										relatedTable, currentLevel + 1,
										newPrefix, rootTable);

								if (r != null)
									result.addAll(r);
							}
						}

					} catch (Exception e) {
						e.printStackTrace();
						logger.error("Error getting cardinality between "
								+ table.getBaseRelationalTableName() + " and "
								+ relatedTable.getBaseRelationalTableName());
					}

				}
			}

		}

		return result;
	}

	public void setRowKeys() {
		System.out.println("Setting rowkeys");
		logger.info("Setting rowkeys");

		for (Table table : schema.getTables()) {
			List<Column> rowkey = fromStringsToColumns(
					usage.getKeyColumns(table.getName()), table);

			table.setAndEncodeKey(rowkey);

			// Key for nested entity
			for (ColumnFamily colFamily : table.getColumnFamilies()) {
				if (!colFamily.getBaseRelationalTable().equals(table.getName())
						&& colFamily.getType() == ColumnFamily.Type.MANY) {
					TableStatus tableMany = db.getTable(colFamily
							.getBaseRelationalTable());

					List<IndexStatus> pkMany = tableMany.getPrimaryKey();
					List<String> pkManyStr = new ArrayList<String>();
					for (IndexStatus index : pkMany) {
						boolean include = true;

						// only include the part of the nested pk that is not
						// part of the host pk
						for (ForeignKeyStatus fk : tableMany.getReferences()) {
							if (fk.getRefTable().equals(
									table.getBaseRelationalTableName())) {
								if (fk.getColumn().equals(index.getColumn())) {
									include = false;
								}
							}
						}

						if (include)
							pkManyStr.add(index.getColumn());
					}

					colFamily.setManyKey(this
							.fromStringsToColumns(pkManyStr, schema
									.getTable(colFamily
											.getBaseRelationalTable())));
				}
			}
		}
	}

	private List<Column> fromStringsToColumns(List<String> columnNames,
			Table table) {
		List<Column> columns = new ArrayList<Column>();

		for (String columnName : columnNames) {
			Column col = table.getColumn(columnName);
			columns.add(col);
		}

		return columns;
	}

	public void useIndexesToDuplicateTables() {
		System.out.println("Creating table duplicates based on indexes");
		logger.info("Creating table duplicates based on indexes");

		List<Table> listOfTables = new ArrayList<Table>(schema.getTables());
		for (Table table : listOfTables) {
			if (!table.isDeleted()) {
				TableStatus ts = db
						.getTable(table.getBaseRelationalTableName());

				List<IndexStatus> primaryKey = ts.getPrimaryKey();
				Map<String, List<IndexStatus>> indexesGrouped = ts
						.getIndexesGrouped();

				for (Map.Entry<String, List<IndexStatus>> indexesEntry : new ArrayList<Map.Entry<String, List<IndexStatus>>>(
						indexesGrouped.entrySet())) {

					List<IndexStatus> indexes = indexesEntry.getValue();

					// extract only the columns of the index
					List<String> indexesStr = new ArrayList<String>();
					boolean isUnique = false;
					for (IndexStatus index : indexes) {
						indexesStr.add(index.getColumn());
						isUnique = index.isUnique();
					}

					if (table.isSubsetOfKey(indexesStr)) {
						// if the current index is contained in the current
						// rowkey, then do not consider it for duplication
						indexesGrouped.remove(indexesEntry.getKey());
					} else {
						// Clone table
						Table clone = table.cloneTable();
						clone.setName(table.getName()
								+ conf.getClonedTableSeparator()
								+ indexesEntry.getKey());
						clone.setAlias(table.getName()
								+ conf.getClonedTableSeparator()
								+ indexesEntry.getKey());
						clone.setIndexName(indexesEntry.getKey());

						// Change row key
						clone.setAndEncodeKey(fromStringsToColumns(indexesStr,
								table));

						// If it is not unique index, then append the primary
						// key to make it unique
						if (!isUnique) {
							for (IndexStatus ix : primaryKey) {
								indexesStr.add(ix.getColumn());
							}
							// TO-DO Remove fromStringsToColumns
							clone.setAndEncodeKey(fromStringsToColumns(
									indexesStr, table));

						}

						schema.addTable(
								table.getName()
										+ conf.getClonedTableSeparator()
										+ indexesEntry.getKey(), clone);
					}
				}
			}
		}
	}

	public void generateSQLStatementsInColFamilies() {

		for (Table table : schema.getTables()) {

			for (ColumnFamily colFamily : table.getColumnFamilies()) {
				Query query;

				if (colFamily.getType() != ColumnFamily.Type.EXT) {
					if (table.getBaseRelationalTableName().equals(
							colFamily.getBaseRelationalTable())) {
						List<String> relation = new ArrayList<String>();
						relation.add(table.getBaseRelationalTableName());
						query = new Query(new ArrayList<QueryAttribute>(),
								relation, new HashSet<QueryFilter>(), 0);
					} else {
						List<ForeignKeyStatus> fk = db.getForeignKey(colFamily
								.getName());
						query = getJoinQuery(fk);
					}
				} else {
					query = new Query();
					String[] tableRelations = colFamily.getName().split(
							conf.getExtColFamilyDelimiter());

					for (int i = 0; i < tableRelations.length; i++) {
						List<ForeignKeyStatus> fk = db
								.getForeignKey(tableRelations[i]);
						addJoinToQuery(query, fk);
					}
				}

				addKeyProjectionToQuery(query, table.getKey(),
						table.getBaseRelationalTableName());
				addProjectionsToQuery(query, colFamily, table);

				colFamily.setSql(query.toString());
			}
		}
	}

	private void addKeyProjectionToQuery(Query query, List<RowkeyColumn> keys,
			String tableName) {
		for (RowkeyColumn key : keys) {
			if (key instanceof RowkeySpatialColumn) {
				RowkeySpatialColumn spatialColumn = (RowkeySpatialColumn) key;

				QueryAttribute qa = new QueryAttribute(tableName, spatialColumn
						.getLatitude().getName());
				query.addProjection(qa);
				qa = new QueryAttribute(tableName, spatialColumn.getLongitude()
						.getName());
				query.addProjection(qa);
			} else {
				QueryAttribute qa = new QueryAttribute(tableName, key.getName());
				query.addProjection(qa);
			}
		}
	}

	private void addProjectionsToQuery(Query query, ColumnFamily colFamily,
			Table table) {
		for (Column column : colFamily.getColumns()) {
			if (column.isChecked()) {
				QueryAttribute qa = new QueryAttribute(column.getTable(),
						column.getName());
				query.addProjection(qa);
			}
		}

		String extBaseTableName = "";
		ColumnFamily.Type extBaseTableType = null;
		if (colFamily.getType() == ColumnFamily.Type.EXT) {
			String firstRelation = colFamily.getName().split(
					conf.getExtColFamilyDelimiter())[0];
			extBaseTableName = firstRelation;
			extBaseTableType = table.getColumnFamily(extBaseTableName)
					.getType();
		}

		if (colFamily.getType() == ColumnFamily.Type.MANY
				|| (colFamily.getType() == ColumnFamily.Type.EXT && extBaseTableType == ColumnFamily.Type.MANY)) {
			List<Column> manyKey = null;
			if (colFamily.getType() == ColumnFamily.Type.MANY) {
				manyKey = colFamily.getManyKey();
			} else {
				manyKey = table.getColumnFamily(extBaseTableName).getManyKey();
			}

			for (Column column : manyKey) {
				QueryAttribute qa = new QueryAttribute(column.getTable(),
						column.getName());
				query.addProjection(qa);
			}
		}

	}

	private void addJoinToQuery(Query query, List<ForeignKeyStatus> references) {
		Query join = getJoinQuery(references);

		for (QueryAttribute qa : join.getProjections()) {
			query.addProjection(qa);
		}

		for (String relation : join.getRelations()) {
			query.addRelation(relation);
		}

		for (QueryFilter filter : join.getSelections()) {
			query.addSelection(filter);
		}
	}

	private Query getJoinQuery(List<ForeignKeyStatus> references) {
		if (references.size() == 0) {
			return null;
		}

		String tableNameA = references.get(0).getTable();
		String tableNameB = references.get(0).getRefTable();

		TableStatus tableA = db.getTable(tableNameA);
		TableStatus tableB = db.getTable(tableNameB);

		List<String> relations = new ArrayList<String>();
		relations.add(tableA.getName());
		relations.add(tableB.getName());

		Set<QueryFilter> selections = new HashSet<QueryFilter>();

		try {
			for (ForeignKeyStatus reference : references) {
				QueryAttribute left = new QueryAttribute(reference.getTable(),
						reference.getColumn());
				QueryAttribute right = new QueryAttribute(
						reference.getRefTable(), reference.getRefColumn());
				selections.add(new QueryFilter(left, right, "="));
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error getting references between tables");
		}

		return new Query(new ArrayList<QueryAttribute>(), relations, selections);
	}
}
