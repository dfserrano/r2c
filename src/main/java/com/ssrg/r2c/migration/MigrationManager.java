package com.ssrg.r2c.migration;

import com.ssrg.r2c.rdms.Database;

public interface MigrationManager {

	public void migrate(Database db);
}
