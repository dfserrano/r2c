package com.ssrg.r2c.coloriented;

public class Column implements Cloneable {

	protected String name;
	private String alias;
	private String table;
	private String dataType;
	private boolean checked;
	
	public Column(String table, String name, String type, boolean checked) {
		this.name = name;
		this.alias = name;
		this.table = table;
		this.dataType = type;
		this.checked = checked;
	}

	public String getName() {
		return name;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public String getType() {
		return dataType;
	}

	public String getTable() {
		return table;
	}
	
	public boolean isChecked() {
		return checked;
	}
	
	public void setChecked(boolean s) {
		checked = s;
	}
	
	public boolean isLatitude() {
		if (dataType.equals("float")
				&& (name.equals("lat") || dataType.equals("latitude"))) {
			return true;
		}

		return false;
	}

	public boolean isLongitude() {
		if (dataType.equals("float")
				&& (name.equals("lng") || dataType.equals("longitude") || dataType
						.equals("long"))) {
			return true;
		}

		return false;
	}
	
	public boolean isGeo() {
		if (isLatitude() || isLongitude()) {
			return true;
		}
		
		return false;
	}

	public boolean isTemporal() {
		if (dataType.equals("date") || dataType.equals("time")
				|| dataType.equals("datetime")) {
			return true;
		}

		return false;
	}


	@Override
	public String toString() {
		return name;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		Column clone = new Column(table, name, dataType, checked);
		
		return clone;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	
}
