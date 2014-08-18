package com.ssrg.r2c.rdms;

import java.sql.ResultSet;

public interface Database {

	public String getName();
	public ResultSet executeQuery(String sqlQuery);
	public ResultSet executeInnerQuery(String sqlQuery);
	public ResultSet executeLargeQuery(String sqlQuery);
	public void disconnect();
}
