package com.ssrg.r2c;

import java.util.HashSet;
import java.util.Set;

import com.ssrg.r2c.coloriented.Schema;
import com.ssrg.r2c.migration.HBaseBuilder;
import com.ssrg.r2c.migration.HBaseMigrationManager;
import com.ssrg.r2c.migration.MappingDirector;
import com.ssrg.r2c.migration.MigrationManager;
import com.ssrg.r2c.rdms.Database;
import com.ssrg.r2c.rdms.DatabaseInfo;
import com.ssrg.r2c.rdms.MySQL;
import com.ssrg.r2c.rdms.MySQLInfo;
import com.ssrg.r2c.rdms.metadata.DatabaseStatus;
import com.ssrg.r2c.usage.sql.QueryUsage;


public class Main {

	public static final String ENV = "DEBUG";

	public static void main(String[] args) throws Exception {
		QueryUsage usage;
		DatabaseStatus db;
		Schema schema;
		
		Configuration conf = new Configuration();
		
		//String dbName = "DS2";
		String dbName = "coupons";

		Set<String> excludedTables = new HashSet<String>();
		excludedTables.add("CUST_HIST");

		Database mySqlDatabase = new MySQL("jdbc:mysql://localhost/" + dbName,
				"r2c", "reltocolumn", dbName);
		DatabaseInfo info = new MySQLInfo((MySQL) mySqlDatabase, excludedTables);

		db = info.getDatabaseStatus();
		//usage = new QueryUsage(db, "logs/queries_dell.log");
		usage = new QueryUsage(db);

		HBaseBuilder builder = new HBaseBuilder(db, usage, conf);
		MappingDirector director = new MappingDirector();

		director.constructSchema(builder);
		schema = builder.getSchema();

		System.out.println(schema);

		MigrationManager migrator = new HBaseMigrationManager(schema, conf);
		migrator.migrate(mySqlDatabase);
		
	}
}
