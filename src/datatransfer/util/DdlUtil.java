/*
 * 
 * 
 */
package datatransfer.util;

import java.sql.Connection;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.databasemediators.DatabaseDriver;
import org.executequery.datasource.ConnectionManager;
import org.executequery.repository.DatabaseDriverRepository;
import org.executequery.repository.RepositoryCache;

/**
 * 
 * @author zgw@dongying.pbc
 */
public class DdlUtil {

	public static Platform getPlatform(DatabaseConnection databaseConnection) {
		javax.sql.DataSource dc = ConnectionManager
				.getDataSource(databaseConnection);
		return PlatformFactory.createNewPlatformInstance(ConnectionManager
				.getDataSource(databaseConnection));
	}

	/**
	 * 获取Database
	 * 
	 * @param dataSource
	 * @return
	 */
	public static Database getDatabase(DatabaseConnection databaseConnection) throws Exception{
		Platform platform = getPlatform(databaseConnection);
		DatabaseDriver databaseDriver = ((DatabaseDriverRepository) RepositoryCache
				.load(DatabaseDriverRepository.REPOSITORY_ID))
				.findById(databaseConnection.getDriverId());
		if (databaseDriver.isUseSchema()) {
			if (databaseConnection.getSchema().trim().length() > 1) {
				databaseConnection.setSchema(databaseConnection.getSchema());
			}
			return platform.readModelFromDatabaseWithSchema(databaseConnection);
//			return platform.readModelFromDatabase(
//					databaseConnection.getSourceName(), null,
//					databaseConnection.getSchema(), null);
		} else {
			return platform.readModelFromDatabaseWithNoSchema(databaseConnection);
//			return platform.readModelFromDatabase(databaseConnection
//					.getSourceName());
		}
	}

	public static Database getDatabase(DatabaseConnection databaseConnection,
			Connection con) throws Exception {

		Platform platform = getPlatform(databaseConnection);
		DatabaseDriver databaseDriver = ((DatabaseDriverRepository) RepositoryCache
				.load(DatabaseDriverRepository.REPOSITORY_ID))
				.findById(databaseConnection.getDriverId());
		if (databaseDriver.isUseSchema()) {
			if (databaseConnection.getSchema().trim().length() > 1) {
				databaseConnection.setSchema(databaseConnection.getSchema());
			}

			return platform.readModelFromDatabaseWithSchema(databaseConnection);

		} else {

			return platform
					.readModelFromDatabaseWithNoSchema(databaseConnection);

		}
	}

	/**
	 * 创建数据库表，返回创建失败的表(用,分割) 创建表前先drop
	 * 
	 * @param dataSource
	 * @param database
	 */
	public static String createTables(DatabaseConnection databaseConnection,
			Database database) {
		String tablesFailed = "";
		Platform platform = PlatformFactory
				.createNewPlatformInstance(ConnectionManager
						.getDataSource(databaseConnection));
		platform.createTables(ConnectionManager.getConnection(databaseConnection),database, true, true);
		Table[] tablesAll = database.getTables(); // 所有需要创建的表
		Table[] tablesCreated = platform.readModelFromDatabase(
				databaseConnection.getSourceName()).getTables();// 创建成功的表

		for (Table t1 : tablesAll) {
			boolean success = false;
			for (Table t2 : tablesCreated) {
				if (t1.getName().trim().equalsIgnoreCase(t2.getName().trim())) {
					success = true;
					break;
				}
			}
			if (!success) {
				tablesFailed += t1.getName().trim() + ",";
			}
		}
		return tablesFailed;
	}
}
