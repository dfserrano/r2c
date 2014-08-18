package com.ssrg.r2c.migration;

public class MappingDirector {

	public void constructSchema(ColumnOrientedBuilder builder) {
		
		builder.initializeSchema();
		builder.mergeOneToOneRelationships();
		builder.mergeOneToManyRelationships();
		builder.addColumnsToCurrentSchema();
		builder.setRowKeys();
		builder.extendSchema();
		builder.useIndexesToDuplicateTables();
		builder.generateSQLStatementsInColFamilies();
	}
}
