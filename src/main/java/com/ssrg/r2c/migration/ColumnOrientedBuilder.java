package com.ssrg.r2c.migration;
import com.ssrg.r2c.coloriented.Schema;


public interface ColumnOrientedBuilder {

	public Schema getSchema();
	
	public void initializeSchema();
	public void mergeOneToOneRelationships();
	public void mergeOneToManyRelationships();
	public void addColumnsToCurrentSchema();
	public void extendSchema();
	public void setRowKeys();
	public void useIndexesToDuplicateTables();
	public void generateSQLStatementsInColFamilies();
}
