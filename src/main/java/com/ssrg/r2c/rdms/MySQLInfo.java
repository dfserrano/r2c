package com.ssrg.r2c.rdms;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import com.ssrg.r2c.rdms.metadata.ColumnStatus;
import com.ssrg.r2c.rdms.metadata.DatabaseStatus;
import com.ssrg.r2c.rdms.metadata.ForeignKeyStatus;
import com.ssrg.r2c.rdms.metadata.IndexStatus;
import com.ssrg.r2c.rdms.metadata.TableStatus;


public class MySQLInfo implements DatabaseInfo {

	private Set<String> excludedTables;
	private Database database;
	private String type = "MYSQL";

	public MySQLInfo(MySQL database) {
		this.database = database;

		this.excludedTables = new HashSet<String>();
	}

	public MySQLInfo(MySQL database, Set<String> excludedTables) {
		this(database);
		this.excludedTables = excludedTables;
	}
	
	public String getType() {
		return type;
	}

	public DatabaseStatus getDatabaseStatus() {
		DatabaseStatus ds = new DatabaseStatus(database.getName());

		ds.setTables(this.getTablesInfo());
		database.disconnect();

		// this.calculateDatabaseStatistics(ds);

		return ds;
	}

	private Hashtable<String, TableStatus> getTablesInfo() {
		String statusTableSQL = "SHOW TABLE STATUS FROM " + database.getName();
		Hashtable<String, TableStatus> tables = new Hashtable<String, TableStatus>();

		try {
			ResultSet rs = database.executeQuery(statusTableSQL);
			
			while (rs.next()) {
				String name = rs.getString("Name");
				int rows = rs.getInt("Rows");
				Date createDate = rs.getDate("Create_time");
				Date lastUpdate = rs.getDate("Update_time");

				if (!excludedTables.contains(name)) {
					try {
						List<ColumnStatus> columns = getColumnInfo(name);
						List<IndexStatus> indexes = getIndexInfo(name);
						List<ForeignKeyStatus> foreignKeys = getForeignKeyInfo(name);

						TableStatus ts = new TableStatus(name, rows,
								createDate, lastUpdate, indexes, foreignKeys,
								columns);

						tables.put(name, ts);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return tables;
	}

	private List<IndexStatus> getIndexInfo(String tableName) {
		String statusIndexSQL = "SHOW INDEXES FROM " + tableName;
		List<IndexStatus> indexes = new ArrayList<IndexStatus>();

		try {
			ResultSet rs = database.executeQuery(statusIndexSQL);

			while (rs.next()) {
				String name = rs.getString("Key_name");
				boolean nonUnique = rs.getBoolean("Non_unique");
				String column = rs.getString("Column_name");
				int cardinality = rs.getInt("Cardinality");
				boolean isNull = rs.getBoolean("Null");

				IndexStatus is = new IndexStatus(name, !nonUnique, column,
						cardinality, isNull);
				indexes.add(is);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return indexes;
	}

	private List<ForeignKeyStatus> getForeignKeyInfo(String tableName) {
		String statusForeignKeySQL = "SELECT constraint_name, table_name, "
				+ "column_name, referenced_table_name, "
				+ "referenced_column_name "
				+ "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
				+ "WHERE TABLE_NAME =  '" + tableName + "'";
		List<ForeignKeyStatus> foreignKeys = new ArrayList<ForeignKeyStatus>();

		try {
			ResultSet rs = database.executeQuery(statusForeignKeySQL);

			while (rs.next()) {
				String name = rs.getString("constraint_name");
				String table = rs.getString("table_name");
				String column = rs.getString("Column_name");
				String refTable = rs.getString("referenced_table_name");
				String refColumn = rs.getString("referenced_column_name");

				if (refTable != null) {
					ForeignKeyStatus fs = new ForeignKeyStatus(name, table,
							column, refTable, refColumn);

					String typeOfRelSQL = "SELECT COUNT( " + column
							+ " ) AS cnt " + "FROM  " + table + " "
							+ "GROUP BY " + column + " " + "ORDER BY COUNT( "
							+ column + " ) DESC " + "LIMIT 0 , 1";
					ResultSet rsi = database.executeQuery(typeOfRelSQL);

					if (rsi.next()) {
						int count = rsi.getInt("cnt");

						if (count == 1) {
							// System.out.println("ONE TO ONE");
							fs.setType(ForeignKeyStatus.Type.ONE);
						} else {
							// System.out.println("ONE TO MANY");
							fs.setType(ForeignKeyStatus.Type.MANY);
						}
					} else {
						fs.setType(ForeignKeyStatus.Type.MANY);
					}

					foreignKeys.add(fs);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return foreignKeys;
	}

	private List<ColumnStatus> getColumnInfo(String tableName) {
		String statusIndexSQL = "SHOW COLUMNS FROM " + tableName;
		List<ColumnStatus> columns = new ArrayList<ColumnStatus>();

		try {
			ResultSet rs = database.executeQuery(statusIndexSQL);

			while (rs.next()) {
				String name = rs.getString("Field");
				String key = rs.getString("Key");
				String type = rs.getString("Type");

				ColumnStatus cs = new ColumnStatus(name, key.equals("PRI"),
						type);
				;
				columns.add(cs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return columns;
	}

//	private void calculateDatabaseStatistics(DatabaseStatus ds) {
//		float sum = 0;
//		float q1, q2, q3 = 0;
//
//		Collection<TableStatus> tables = ds.getTables();
//		float[] rates = new float[tables.size()];
//		int i = 0;
//
//		for (TableStatus ts : tables) {
//			float rate = ts.getInsertionRate();
//			sum += rate;
//			rates[i++] = rate;
//		}
//
//		float avg = sum / tables.size();
//
//		Arrays.sort(rates);
//
//		q2 = getMedian(rates, 0, rates.length - 1);
//		if (rates.length / 2.0 == (int) (rates.length / 2.0)) {
//			q1 = getMedian(rates, 0, (int) ((rates.length / 2) - 2));
//			q3 = getMedian(rates, (int) ((rates.length / 2) + 1),
//					rates.length - 1);
//		} else {
//			q1 = getMedian(rates, 0, (int) (Math.floor(rates.length / 2) - 1));
//			q3 = getMedian(rates, (int) (Math.ceil(rates.length / 2)),
//					rates.length - 1);
//		}
//
//		ds.setAvgInsertRate(avg);
//		ds.setQuartileInsertRate(1, q1);
//		ds.setQuartileInsertRate(2, q2);
//		ds.setQuartileInsertRate(3, q3);
//	}
//
//	private float getMedian(float[] a, int start, int end) {
//		Arrays.sort(a);
//		float median = 0;
//		int length = (end - start) + 1;
//
//		if (length / 2.0 == (int) (length / 2.0)) {
//			median = (a[start + (int) (length / 2 - 1)] + a[start
//					+ (int) (length / 2)]) / 2;
//		} else {
//			median = a[start + (int) Math.floor(length / 2)];
//		}
//
//		return median;
//	}
}
