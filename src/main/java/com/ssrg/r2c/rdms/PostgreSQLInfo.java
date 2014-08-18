package com.ssrg.r2c.rdms;

import java.sql.ResultSet;

import com.ssrg.r2c.rdms.metadata.DatabaseStatus;


public class PostgreSQLInfo implements DatabaseInfo {

	private String type = "POSTGRESQL";

	public PostgreSQLInfo(String url, String username, String password,
			String dbName) {
		throw new UnsupportedOperationException();
	}

	public String getType() {
		return type;
	}

	public DatabaseStatus getDatabaseStatus() {
		throw new UnsupportedOperationException();
	}

	public ResultSet executeQuery(String sqlQuery) {
		throw new UnsupportedOperationException();
	}
}
