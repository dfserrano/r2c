package com.ssrg.r2c.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import ch.hsr.geohash.GeoHash;

import com.ssrg.r2c.coloriented.ColumnFamily;
import com.ssrg.r2c.coloriented.Record;
import com.ssrg.r2c.coloriented.Schema;
import com.ssrg.r2c.coloriented.Table;
import com.ssrg.r2c.util.Utils;

public class HBase {

	private Configuration conf;
	private HConnection connection;
	private HTableInterface tableInterface;

	public HBase() {
		conf = HBaseConfiguration.create();

		// conf.set("hbase.zookeeper.quorum", "localhost");
		// conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public HBase(String hostname, int port) {
		this();

		conf.set("hbase.zookeeper.quorum", hostname);
		conf.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
	}

	public void createTables(Schema schema) {
		try {
			HBaseAdmin hbase = new HBaseAdmin(conf);

			for (Table table : schema.getTables()) {
				System.out.println(table.getAlias());

				// drop if exists
				if (hbase.tableExists(table.getAlias())) {
					hbase.disableTable(table.getAlias());
					hbase.deleteTable(table.getAlias());
				}

				HTableDescriptor desc = new HTableDescriptor(table.getAlias());

				for (ColumnFamily colFamily : table.getColumnFamilies()) {
					System.out.println("\t" + colFamily.getAlias());
					HColumnDescriptor cf = new HColumnDescriptor(colFamily
							.getAlias().getBytes());
					desc.addFamily(cf);
				}

				hbase.createTable(desc);
			}

			hbase.close();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void insertRecord(Record record) {
		try {
			if (connection == null || connection.isClosed()) {
				connection = HConnectionManager.createConnection(conf);
			}

			if (tableInterface == null
					|| !tableInterface.getTableDescriptor().getNameAsString()
							.equals(record.getTableName())) {
				tableInterface = connection.getTable(record.getTableName());
			}

			Put p = makePut(record);
			tableInterface.put(p);
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Put makePut(Record record) {
		Put p = new Put(record.getRowkeyAsBytes());
		p.add(record.getColumnFamilyAsBytes(), record.getColumnAsBytes(),
				record.getValueAsBytes());
		return p;
	}

	public void scan() {
		throw new UnsupportedOperationException();
	}

	public void get(String tableName, byte[] rowkey, String colFamily,
			String column) throws IOException {
		if (connection == null || connection.isClosed()) {
			connection = HConnectionManager.createConnection(conf);
		}

		long start = System.nanoTime();
		HTableInterface table = connection.getTable(tableName);

		// Simple GET
		Get g = new Get(rowkey);
		Result r = table.get(g);

		String result = Bytes.toString(r.getValue(Bytes.toBytes(colFamily),
				Bytes.toBytes(column)));
		long end = System.nanoTime();

		System.out.println("Time: " + ((end - start) * 1E-9) + " s.");

		System.out.println(result);
	}

	public void get(String tableName, byte[] rowkey, String colFamily)
			throws IOException {
		if (connection == null || connection.isClosed()) {
			connection = HConnectionManager.createConnection(conf);
		}

		HTableInterface table = connection.getTable(tableName);

		// Simple GET
		Get g = new Get(rowkey);
		Result r = table.get(g);

		Map<byte[], byte[]> resultMap = r
				.getFamilyMap(Bytes.toBytes(colFamily));

		printResult(resultMap);
	}

	public List<Result> scanPrefix(String tableName, String prefixStr)
			throws IOException {
		return scanPrefix(tableName, prefixStr, null);
	}

	public List<Result> scanPrefix(String tableName, String prefixStr, String colFamily)
			throws IOException {
		byte[] prefix = Bytes.toBytes(prefixStr);
		
		return scanPrefix(tableName, prefix, colFamily, null);
	}
	
	public List<Result> scanPrefix(String tableName, byte[] prefix, String colFamily, byte[] colPrefix)
			throws IOException {
		if (connection == null || connection.isClosed()) {
			connection = HConnectionManager.createConnection(conf);
		}

		List<Result> results = new ArrayList<Result>();
		
		HTableInterface table = connection.getTable(tableName);

		Scan scan = new Scan(prefix);
		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		scan.setFilter(prefixFilter);
		
		if (colPrefix != null && colPrefix.length > 0) {
			ColumnPrefixFilter colPrefixFilter = new ColumnPrefixFilter(colPrefix);
			scan.setFilter(colPrefixFilter);
		}

		if (colFamily != null) {
			scan.addFamily(Bytes.toBytes(colFamily));
		}

		ResultScanner result = table.getScanner(scan);
		printResult(result);
		
		for (Result r : result) {
			results.add(r);
		}
		
		System.out.println("****");
		
		return results;
	}

	public List<Result> scanGeohash(String tableName, String colFamily, double latitude, double longitude, int precision)
			throws IOException {
		if (connection == null || connection.isClosed()) {
			connection = HConnectionManager.createConnection(conf);
		}

		List<Result> results = new ArrayList<Result>();
		
		GeoHash geohash = GeoHash.withCharacterPrecision(latitude, longitude,
				precision);
		results.addAll(scanPrefix(tableName, geohash.toBase32(), colFamily));
		
		GeoHash[] adjacents = geohash.getAdjacent();

		for (int i = 0; i < adjacents.length; i++) {
			results.addAll(scanPrefix(tableName, adjacents[i].toBase32(), colFamily));
		}
		
		return results;
	}

	public void printResult(Map<byte[], byte[]> resultMap) {
		for (Entry<byte[], byte[]> entry : resultMap.entrySet()) {
			System.out.println(Bytes.toString(entry.getKey()) + " = "
					+ Bytes.toString(entry.getValue()));
		}
	}

	public void printResult(ResultScanner result) {
		for (Result res : result) {
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = res
					.getMap();

			System.out.println("key: " + Utils.bytesToBinaryString(res.getRow()));

			for (byte[] key1 : map.keySet()) {
				System.out.println("\t" + Bytes.toString(key1));

				for (byte[] key2 : map.get(key1).keySet()) {
					System.out.println("\t\t" + Bytes.toString(key2));

					for (Long key3 : map.get(key1).get(key2).keySet()) {
						System.out.println("\t\t\t"
								+ key3
								+ " = "
								+ Bytes.toString(map.get(key1).get(key2)
										.get(key3)));
					}
				}
			}
		}
	}

	public void close() {
		try {
			if (tableInterface != null) {
				tableInterface.close();
			}

			if (connection != null) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
