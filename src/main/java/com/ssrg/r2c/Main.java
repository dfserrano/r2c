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
		conf.setTimestampBytePrecision(Configuration.TIMESTAMP_PRECISION_HOUR);
		//conf.setHbaseHostname("node1.ssrg.ca:9000");
		//conf.setZookeeperQuorum("node1.ssrg.ca");
		//conf.setZookeeperPort(2181);
		conf.setHbaseHostname("localhost:9000");
		conf.setZookeeperQuorum("localhost");
		conf.setZookeeperPort(2181);
		
		String dbName = "ds2";
		//String dbName = "coupons";

		Set<String> excludedTables = new HashSet<String>();
		excludedTables.add("cust_hist");
		excludedTables.add("reorder");

		//Database mySqlDatabase = new MySQL("jdbc:mysql://10.1.1.76/" + dbName,	"r2c", "reltocolumn", dbName);
		Database mySqlDatabase = new MySQL("jdbc:mysql://localhost/" + dbName,	"root", "Giba007G", dbName);
		DatabaseInfo info = new MySQLInfo((MySQL) mySqlDatabase, excludedTables);

		db = info.getDatabaseStatus();
		
		// temporal
		if (args.length > 0) {
			// logs/queries_dell.log
			usage = new QueryUsage(db, args[0]);
		} else {
			usage = new QueryUsage(db);
		}

		HBaseBuilder builder = new HBaseBuilder(db, usage, conf);
		MappingDirector director = new MappingDirector();

		director.constructSchema(builder);
		schema = builder.getSchema();

		System.out.println(schema);

		//MigrationManager migrator = new HBaseMigrationManager(schema, conf);
		//migrator.migrate(mySqlDatabase);
		
	}
}
