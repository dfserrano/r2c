package com.ssrg.r2c.coloriented;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class Record {

	private String table;
	private String columnFamily;
	private List<RecordValue> column;
	private List<RecordValue> rowkey;
	private RecordValue value;

	public Record(String table, String columnFamily, List<RecordValue> column,
			List<RecordValue> rowkey, RecordValue value) {
		this.rowkey = rowkey;
		this.table = table;
		this.columnFamily = columnFamily;
		this.column = column;
		this.value = value;
	}
	
	public byte[] getTableAsBytes() {
		return Bytes.toBytes(table);
	}
	
	public String getTableName() {
		return table;
	}

	public byte[] getColumnFamilyAsBytes() {
		return Bytes.toBytes(columnFamily);
	}

	public byte[] getColumnAsBytes() {
		return listToBytes(column);
	}

	public byte[] getRowkeyAsBytes() {
		return listToBytes(rowkey);
	}

	public byte[] getValueAsBytes() {
		return value.getValueBytes();
	}



	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append(table + "." + columnFamily + ".");
		
		for (RecordValue rv : column) {
			sb.append(rv.getValueString() + ":");
		}
		
		sb.delete(sb.length()-1, sb.length());
		sb.append(" (");
		
		for (RecordValue rv : rowkey) {
			sb.append(rv.getValueString() + ":");
		}
		
		sb.delete(sb.length()-1, sb.length());
		sb.append(") = ");
		sb.append(value.getValueString());
		
		return sb.toString();
	}

	private byte[] listToBytes(List<RecordValue> values) {
		int totalBytes = 0;

		for (RecordValue rv : values) {
			totalBytes += rv.getValueBytes().length;
		}

		byte[] byteval = new byte[totalBytes];
		int offset = 0;

		for (RecordValue rv : values) {
			offset = Bytes.putBytes(byteval, offset, rv.getValueBytes(), 0,
					rv.getValueBytes().length);
		}

		return byteval;
	}
}
