package com.ssrg.r2c.migration;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import ch.hsr.geohash.GeoHash;

import com.ssrg.r2c.Configuration;
import com.ssrg.r2c.coloriented.Column;
import com.ssrg.r2c.coloriented.ColumnFamily;
import com.ssrg.r2c.coloriented.Record;
import com.ssrg.r2c.coloriented.RecordValue;
import com.ssrg.r2c.coloriented.RowkeyColumn;
import com.ssrg.r2c.coloriented.RowkeySpatialColumn;
import com.ssrg.r2c.coloriented.Schema;
import com.ssrg.r2c.coloriented.Table;
import com.ssrg.r2c.rdms.Database;
import com.ssrg.r2c.util.Utils;

public class HBaseMigrationManager implements MigrationManager {

	private Schema schema;
	Configuration conf;

	static final Logger logger = Logger.getLogger(HBaseMigrationManager.class);

	public HBaseMigrationManager(Schema schema, Configuration conf) {
		this.schema = schema;
		this.conf = conf;
	}

	public void migrate(Database database) {
		HBase hbase = new HBase();

		try {
			System.out.println("Creating HBase schema...");
			logger.info("Creating HBase schema");
			hbase.createTables(schema);

			for (Table table : schema.getTables()) {
				System.out.println("Migrating table " + table.getName());
				logger.info("Migrating table " + table.getName());

				if (!table.isDeleted()) {
					System.out.println("Migrating table " + table.getName());
					logger.info("Migrating table " + table.getName());

					for (ColumnFamily colFamily : table.getColumnFamilies()) {
						System.out.println("Migrating column family "
								+ colFamily.getName());
						logger.info("Migrating column family "
								+ colFamily.getName());

						int counter = 0;
						ResultSet rs = database.executeLargeQuery(colFamily
								.getSql());

						try {
							while (rs.next()) {
								counter++;
								if (counter % 100000 == 0) {
									System.out.println(" Record " + counter);
									logger.info(" Record " + counter);
								}

								List<RecordValue> rowkey = extractRowkey(rs,
										table.getKey());

								long timestamp = getRowkeyTimestamp(rs,
										table.getKey());
								long roundedTimestamp = Utils.roundDownTimestamp(timestamp, conf);

								boolean hasTimestamp = (timestamp > 0) ? true
										: false;

								ColumnFamily extColumnFamily = null;
								if (colFamily.getType() == ColumnFamily.Type.EXT) {
									String extBaseTableName = colFamily
											.getName()
											.split(conf
													.getExtColFamilyDelimiter())[0];
									extColumnFamily = table
											.getColumnFamily(extBaseTableName);
								}

								List<RecordValue> manyPrefix = extractManyKey(
										rs, colFamily, extColumnFamily);

								for (Column column : colFamily.getColumns()) {
									if (column.isChecked()) {
										List<RecordValue> columnKey = new ArrayList<RecordValue>();

										// last part of timestamp in rowkey
										if (hasTimestamp) {
											long difference = timestamp
													- roundedTimestamp;

											byte[] byteDiff = Bytes
													.toBytes(difference);
											byte[] byteDiffFinal = new byte[conf
													.getTimestampNumBytesPrecision()];
											byteDiffFinal = Arrays
													.copyOfRange(
															byteDiff,
															8 - conf.getTimestampNumBytesPrecision(),
															8);

											RecordValue valTimestamp = new RecordValue(
													Utils.bytesToBinaryString(byteDiffFinal),
													byteDiffFinal);

											columnKey.add(valTimestamp);
										}

										// copy many keys
										for (RecordValue rc : manyPrefix) {
											columnKey.add(rc);
										}

										// name of column
										RecordValue columnName = new RecordValue(
												column.getAlias(),
												Bytes.toBytes(column.getName()));
										columnKey.add(columnName);

										// get cell value
										RecordValue value = getRecordValue(
												rs,
												column.getTable()
														+ conf.getTableSeparator()
														+ column.getName(),
												column.getType());

										Record record = new Record(
												table.getName(),
												colFamily.getName(), columnKey,
												rowkey, value);
										hbase.insertRecord(record);
										// System.out.println("\t" + record);
									}
								}
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}

						rs.close();

						System.out.println("Finished migrating column family "
								+ colFamily.getName());
						logger.info("Finished migrating column family "
								+ colFamily.getName());
					}
				} else {
					System.out.println("Skipping table " + table.getName());
					logger.info("Skipping table " + table.getName());
				}

				System.out.println("Finished migrating table "
						+ table.getName());
				logger.info("Finished migrating table " + table.getName());

			}

			// final flush of whatever is left
			hbase.flushBuffer();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hbase != null) {
				hbase.close();
			}
		}
	}

	private List<RecordValue> extractManyKey(ResultSet rs,
			ColumnFamily curColFamily, ColumnFamily extColFamily)
			throws Exception {
		List<RecordValue> manyPrefixList = new ArrayList<RecordValue>();

		if (curColFamily.getType() == ColumnFamily.Type.MANY
				|| (curColFamily.getType() == ColumnFamily.Type.EXT && extColFamily
						.getType() == ColumnFamily.Type.MANY)) {

			List<Column> manyKey = null;
			if (curColFamily.getType() == ColumnFamily.Type.MANY) {
				manyKey = curColFamily.getManyKey();
			} else {
				if (extColFamily != null)
					manyKey = extColFamily.getManyKey();
			}

			for (Column manyKeyCol : manyKey) {
				manyPrefixList.add(getRecordValue(rs, manyKeyCol.getTable()
						+ conf.getTableSeparator() + manyKeyCol.getName(),
						manyKeyCol.getType()));
			}
		}

		return manyPrefixList;
	}

	private long getRowkeyTimestamp(ResultSet rs, List<RowkeyColumn> keys) {
		for (RowkeyColumn keyColumn : keys) {

			if (keyColumn.getEncoding() == RowkeyColumn.Encoding.TIMESTAMP) {
				try {
					long ts = rs.getTimestamp(
							keyColumn.getTable() + conf.getTableSeparator()
									+ keyColumn.getName()).getTime();

					return ts;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return -1;
	}

	private List<RecordValue> extractRowkey(ResultSet rs,
			List<RowkeyColumn> keys) throws Exception {
		List<RecordValue> rowkeyList = new ArrayList<RecordValue>();

		for (RowkeyColumn keyColumn : keys) {

			if (keyColumn.getEncoding() == RowkeyColumn.Encoding.GEOHASH) {
				try {
					RowkeySpatialColumn spatialColumn = (RowkeySpatialColumn) keyColumn;

					double lat = rs.getDouble(keyColumn.getTable()
							+ conf.getTableSeparator()
							+ spatialColumn.getLatitude().getName());
					double lng = rs.getDouble(keyColumn.getTable()
							+ conf.getTableSeparator()
							+ spatialColumn.getLongitude().getName());
					GeoHash geohash = getGeoHash(lat, lng);

					byte[] geohashBytes = Bytes.toBytes(geohash.toBase32());

					RecordValue val = new RecordValue(geohash.toBase32(),
							geohashBytes);
					rowkeyList.add(val);

				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else if (keyColumn.getEncoding() == RowkeyColumn.Encoding.TIMESTAMP) {
				try {
					long ts = rs.getTimestamp(
							keyColumn.getTable() + conf.getTableSeparator()
									+ keyColumn.getName()).getTime();

					// Round down time
					ts = Utils.roundDownTimestamp(ts, conf);

					byte[] roundedTimestamp = Bytes.toBytes(ts);

					RecordValue val = new RecordValue(
							Utils.bytesToBinaryString(roundedTimestamp),
							roundedTimestamp);
					rowkeyList.add(val);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				RecordValue recordValue = getRecordValue(rs,
						keyColumn.getTable() + conf.getTableSeparator()
								+ keyColumn.getName(), keyColumn.getType());
				rowkeyList.add(recordValue);
			}
		}

		return rowkeyList;
	}

	private GeoHash getGeoHash(double latitude, double longitude) {
		GeoHash geohash = GeoHash.withCharacterPrecision(latitude, longitude,
				12);
		return geohash;
	}

	private RecordValue getRecordValue(ResultSet rs, String columnName,
			String type) throws Exception {
		try {
			if (type.startsWith("int") || type.startsWith("tinyint")
					|| type.startsWith("smallint")
					|| type.startsWith("mediumint")) {
				int val = rs.getInt(columnName);
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("float")) {
				float val = rs.getFloat(columnName);
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("double")) {
				double val = rs.getDouble(columnName);
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("varchar")) {
				String val = rs.getString(columnName);
				return new RecordValue(val, Bytes.toBytes(val));

			} else if (type.startsWith("datetime")) {
				long val = rs.getTimestamp(columnName).getTime();
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("date")) {
				long val = rs.getDate(columnName).getTime();
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("timestamp")) {
				long val = rs.getTime(columnName).getTime();
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("time")) {
				long val = rs.getTimestamp(columnName).getTime();
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else if (type.startsWith("decimal")) {
				BigDecimal val = rs.getBigDecimal(columnName);
				return new RecordValue(String.valueOf(val), Bytes.toBytes(val));

			} else {
				throw new Exception("Type " + type
						+ " is unsupported by the application");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	
}
