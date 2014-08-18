package com.ssrg.r2c.usage;

public class TablePairActivity {

	private String relationName;
	private String tableA;
	private String tableB;
	private long usageIndexA;
	private long usageIndexB;
	private long usageNearIndexA;
	private long usageNearIndexB;

	public TablePairActivity(String tableNameA) {
		this.tableA = tableNameA;
	}
			
	public TablePairActivity(String relationName, String tableNameA,
			String tableNameB) {
		this.relationName = relationName;

		if (tableNameA == null) {
			this.tableA = tableNameB;
		} else if (tableNameB == null) {
			this.tableA = tableNameA;
		} else if (tableNameA != null && tableNameB != null) {
			if (tableNameA.compareTo(tableNameB) > 0) {
				tableA = tableNameB;
				tableB = tableNameA;
			} else {
				tableA = tableNameA;
				tableB = tableNameB;
			}
		} else {
			// throw new
			// Exception("Can't instatiate a TablePairActivity with two null table names");
		}
	}

	public boolean isSingle() {
		if (tableA != null && tableB == null) {
			return true;
		}

		return false;
	}

	public boolean isPair() {
		if (tableA != null && tableB != null) {
			return true;
		}

		return false;
	}

	public String getRelationName() {
		return relationName;
	}

	public String getNameOfTableA() {
		return tableA;
	}

	public void setNameOfTableA(String tableA) {
		this.tableA = tableA;
	}

	public String getNameOfTableB() {
		return tableB;
	}

	public void setNameOfTableB(String tableB) {
		this.tableB = tableB;
	}

	public long getUsageIndexA() {
		return usageIndexA;
	}

	public void setUsageIndexA(long usageIndexA) {
		this.usageIndexA = usageIndexA;
	}

	public void addUsageIndexA(long usageIndexA) {
		this.usageIndexA += usageIndexA;
	}

	public long getUsageIndexB() {
		return usageIndexB;
	}

	public void setUsageIndexB(long usageIndexB) {
		this.usageIndexB = usageIndexB;
	}

	public void addUsageIndexB(long usageIndexB) {
		this.usageIndexB += usageIndexB;
	}

	public long getUsageNearIndexA() {
		return usageNearIndexA;
	}

	public void setUsageNearIndexA(long usageNearIndexA) {
		this.usageNearIndexA = usageNearIndexA;
	}

	public void addUsageNearIndexA(long usageNearIndexA) {
		this.usageNearIndexA += usageNearIndexA;
	}

	public long getUsageNearIndexB() {
		return usageNearIndexB;
	}

	public void setUsageNearIndexB(long usageNearIndexB) {
		this.usageNearIndexB = usageNearIndexB;
	}

	public void addUsageNearIndexB(long usageNearIndexB) {
		this.usageNearIndexB += usageNearIndexB;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tableA == null) ? 0 : tableA.hashCode());
		result = prime * result + ((tableB == null) ? 0 : tableB.hashCode());
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
		TablePairActivity other = (TablePairActivity) obj;
		if (relationName == null
				|| !relationName.equals(other.getRelationName())) {
			return false;
		}
		if (tableA == null) {
			if (other.tableA != null)
				return false;
		} else if (!tableA.equals(other.tableA))
			return false;
		if (tableB == null) {
			if (other.tableB != null)
				return false;
		} else if (!tableB.equals(other.tableB))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TableToTableActivity [tableA=" + tableA + ", tableB=" + tableB
				+ ", usageIndexA=" + usageIndexA + ", usageIndexB="
				+ usageIndexB + ", usageNearIndexA=" + usageNearIndexA
				+ ", usageNearIndexB=" + usageNearIndexB + "]";
	}
}
