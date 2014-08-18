package com.ssrg.r2c.coloriented;

public class RowkeyColumn extends Column {

	public enum Encoding {
		GEOHASH, TIMESTAMP, NONE
	};

	private Encoding encoding;

	public RowkeyColumn(String table, String name, String type, boolean checked) {
		super(table, name, type, checked);
		encoding = Encoding.NONE;
	}

	public RowkeyColumn(Column column) {
		super(column.getTable(), column.getName(), column.getType(), column
				.isChecked());
		encoding = Encoding.NONE;
	}

	public RowkeyColumn(Column column, Encoding enc) {
		super(column.getTable(), column.getName(), column.getType(), column
				.isChecked());
		encoding = enc;
	}

	public Encoding getEncoding() {
		return encoding;
	}

	public void setEncoding(Encoding enc) {
		encoding = enc;
	}
}
