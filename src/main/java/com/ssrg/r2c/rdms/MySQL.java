package com.ssrg.r2c.rdms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQL implements Database {

	private String url;
	private String klass;
	private String username;
	private String password;
	private String dbName;

	private Connection connection;
	private ResultSet rs;
	private Statement stmt;
	private ResultSet rsInner;
	private Statement stmtInner;

	public MySQL(String url, String username, String password, String dbName) {
		this.url = url;
		this.klass = "com.mysql.jdbc.Driver";
		this.username = username;
		this.password = password;

		if (dbName != null && dbName.length() > 0) {
			this.dbName = dbName;
		}
	}

	public String getName() {
		return dbName;
	}

	public ResultSet executeQuery(String sqlQuery) {
		try {
			if (connection == null || connection.isClosed()) {
				this.connect();
			}

			stmt = connection.createStatement();
			stmt.setFetchSize(500);
			rs = stmt.executeQuery(sqlQuery);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
	
	public ResultSet executeLargeQuery(String sqlQuery) {
		try {
			if (connection == null || connection.isClosed()) {
				this.connect();
			}
			
			connection.setAutoCommit(false);
			
			stmt = (Statement) connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
			         java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			
			rs = stmt.executeQuery(sqlQuery);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public ResultSet executeInnerQuery(String sqlQuery) {
		try {
			if (connection == null || connection.isClosed()) {
				this.connect();
			}

			stmtInner = connection.createStatement();
			rsInner = stmt.executeQuery(sqlQuery);

			return rsInner;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private void connect() {
		try {
			Class.forName(klass);
			connection = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void disconnect() {
		try {
			if (stmtInner != null)
				stmtInner.close();

			if (rsInner != null)
				rsInner.close();

			if (stmt != null)
				stmt.close();

			if (rs != null)
				rs.close();

			if (connection != null)
				connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
