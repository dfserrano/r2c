package com.ssrg.r2c.rdms;

import com.ssrg.r2c.rdms.metadata.DatabaseStatus;

public interface DatabaseInfo {

	public DatabaseStatus getDatabaseStatus();
	public String getType();
}
