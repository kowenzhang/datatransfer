package datatransfer.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import datatransfer.config.TransferQuery;

public class TypeChecking {

	public static String doTypeChecking(Connection con, List<TransferQuery> tfqs) {
		StringBuffer sb = new StringBuffer();
		try {
			
			Statement st = null;
			st = con.createStatement();
			ResultSet rs1 = null;
			ResultSet rs2 = null;
			for(TransferQuery tfq :tfqs)
		 {
		    rs1 = st.executeQuery(tfq.getSelSql());
			ResultSetMetaData rsmd1 = rs1.getMetaData();
			rs2 = st.executeQuery("select  " + tfq.getSequncedAlias().substring(1,tfq.getSequncedAlias().length()-2)
					+ "  from  " + tfq.getDestinationTable());
			ResultSetMetaData rsmd2 = rs1.getMetaData();
			for (int i = 0; i < rsmd1.getColumnCount(); i++) {
				if (rsmd1.getColumnType(i) != rsmd2.getColumnType(i))
					sb.append("表" + tfq.getDestinationTable() + "的转换语句第"
							+ (i + 1) + "个字段【" + rsmd1.getColumnName(i)
							+ "】输出类型与目标表不一致" + rsmd1.getColumnType(i) + "!="
							+ rsmd2.getColumnType(i));
			}
		 }

		} catch (Exception e) {
			System.out.println("ocurr error");
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			con = null;
		}

		return sb.toString();
	}
	public static String doTypeChecking(Connection con, TransferQuery tfq) {
		StringBuffer sb = new StringBuffer();
		try {
			
			Statement st = null;
			st = con.createStatement();
			ResultSet rs1 = null;
			ResultSet rs2 = null;
			
		    rs1 = st.executeQuery(tfq.getSelSql());
			ResultSetMetaData rsmd1 = rs1.getMetaData();
			rs2 = st.executeQuery("select  " + tfq.getSequncedAlias().substring(1,tfq.getSequncedAlias().length()-2)
					+ "  from  " + tfq.getDestinationTable());
			ResultSetMetaData rsmd2 = rs1.getMetaData();
			for (int i = 0; i < rsmd1.getColumnCount(); i++) {
				if (rsmd1.getColumnType(i) != rsmd2.getColumnType(i))
					sb.append("表" + tfq.getDestinationTable() + "的转换语句第"
							+ (i + 1) + "个字段【" + rsmd1.getColumnName(i)
							+ "】输出类型与目标表不一致" + rsmd1.getColumnType(i) + "!="
							+ rsmd2.getColumnType(i));
			}
		 
		} catch (Exception e) {
			System.out.println("ocurr error");
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			con = null;
		}

		return sb.toString();
	}
}
