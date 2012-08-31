/*
 * 
 * 
 */
package datatransfer.extract;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.datasource.ConnectionManager;
import org.executequery.repository.RepositoryCache;

import datatransfer.Output;
import datatransfer.config.DataTransferConfig;
import datatransfer.config.UserExtractSql;
import datatransfer.util.DataTransferException;
import datatransfer.util.DdlUtil;

/**
 * 
 * @author zgw@dongying.pbc
 */
public class CopyOfDataExtractTask implements Runnable {

    public static final String STATE_FINISHED = "完成";
    public static final String STATE_RUNNING = "正在运行";
    public static final String STATE_READY = "就绪";
    public static final String STATE_WRONG = "出错停止";
    public static final String STATE_STOPPED = "用户终止";
    private static final String STATE_STOPPING = "正在终止";
    private DatabaseConnection from;
    private DatabaseConnection to;
    private Database toDatabase;
    private Database fromDatabase;
    private Table table;
    private String whereCondition;
    private UserExtractSql userExtractSql;
    private int total;
    private int count = 0;
    private int perCount = 100;
    private int perCountNew = 100;
    private String state;
    private List<TaskListener> taskListenerList;
    private Output output;
    private byte[] lockOfPerCount = new byte[0];

    /*
     * public DataExtractTask(DatabaseConnection from, DatabaseConnection to,
     * Database database, Table table) { this.from = from; this.to = to; //
     * this.database = database; this.table = table;
     * 
     * taskListenerList = new ArrayList<TaskListener>(); this.state =
     * STATE_READY; }
     */
    public CopyOfDataExtractTask(DatabaseConnection from, DatabaseConnection to,
            Database fromDatabase, Database toDatabase, Table table) {
        this.from = from;
        this.to = to;

        this.toDatabase = toDatabase;
        this.fromDatabase = fromDatabase;
        this.table = table;

        taskListenerList = new ArrayList<TaskListener>();
        this.state = STATE_READY;
    }

    public CopyOfDataExtractTask(DatabaseConnection from, DatabaseConnection to,
            Database fromDatabase, Database toDatabase, Table table,
            String whereConditon) {
        this.from = from;
        this.to = to;

        this.toDatabase = toDatabase;
        this.fromDatabase = fromDatabase;
        this.table = table;
        this.whereCondition = whereConditon;
        taskListenerList = new ArrayList<TaskListener>();
        this.state = STATE_READY;
    }

    /**
     * 重新初始化
     */
    public void reInit() {
        if (!getState().equals(STATE_RUNNING)) {

            output.prompt("重新初始化" + getName() + "...");
            Connection conn = null;
            PreparedStatement st = null;
            try {
                calcuteTotal();
                conn = ConnectionManager.getConnection(to);
                if (null != to.getSchema()) {
                    st = conn.prepareStatement("delete from " + to.getSchema()
                            + "." + getTable().getName());
                } else {
                    st = conn.prepareStatement("delete from "
                            + getTable().getName());
                }
                st.execute();
                setCount(0);
                this.setState(STATE_READY);
                output.prompt("重新初始化成功 " + getName());
            } catch (Exception ex) {
                ex.printStackTrace();
                output.error("任务重新初始化出错，datasource:" + to.getName() + ",table:"
                        + table.getName(), ex);
                setState(STATE_WRONG);
            } finally {
                try {
                    st.close();
                    conn.close();
                } catch (SQLException e) {
                }
            }

        } else {
            throw new IllegalStateException(getName() + "正在运行，不能重新初始化");
        }
    }

    public void run() {
        if (getState().equals(STATE_READY)) {
            this.setState(STATE_RUNNING);
        }
        PlatformImplBase fromPlat = null;
        Platform toPlat = null;
        Connection from_connection = null;
        Connection to_connection = null;

        try {
            from_connection = ConnectionManager.getConnection(from);
            to_connection = ConnectionManager.getConnection(to);
            fromPlat = (PlatformImplBase)DdlUtil.getPlatform(getFrom());
            fromPlat.setDatabaseConnection(from);
            toPlat = DdlUtil.getPlatform(getTo());
            calcuteTotal();
            String select = this.getUserExtractSql();
            System.out.println("sql:" + select);

            while (getState().equals(STATE_RUNNING)) {
                int end = count + getPerCount() - 1;
                if (end >= total - 1) {
                    end = total - 1;
                }
                if (count <= end) {
System.out.println("begin==============="+count+"================end:"+end);
                    long fetchBegin = Calendar.getInstance().getTimeInMillis();
                    List list = fromPlat.fetch(from_connection, fromDatabase,
                            select, count, end);
                    System.out.println("list size is:"+list.size());
                  for(Object dbean:list){
                	DynaBean bean=(DynaBean)dbean;
                	String sql_str=toPlat.getInsertSql(toDatabase, bean);
                	System.out.println(sql_str);
                	//toPlat.insert(to_connection,toDatabase, bean);
                }
                    long fetchend = Calendar.getInstance().getTimeInMillis();
                    System.out.println("fetch 耗时：" + table.getName() + ":"
                            + (fetchend - fetchBegin));
                    long insertBegin = Calendar.getInstance().getTimeInMillis();
                    toPlat.insert(to_connection, toDatabase, list,
                            table.getName());
//                    for(Object dbean:list){
//                    	DynaBean bean=(DynaBean)dbean;
//                    	String sql_str=toPlat.getInsertSql(toDatabase, bean);
//                    	System.out.println(sql_str);
//                    	//toPlat.insert(to_connection,toDatabase, bean);
//                    }
                    long insertend = Calendar.getInstance().getTimeInMillis();
                    System.out.println("insert  耗时：" + table.getName() + ":"
                            + (insertend - insertBegin));

//                    output.info("抽取" + table.getName() + ":" + count + "->"
//                            + end);
                    setCount(count + perCount);
                    if (perCount != perCountNew) {
                        perCount = perCountNew;
                        output.prompt(this.getName() + "更改缓存：" + perCount);
                        fireChange();
                    }
                } else {
                    setCount(total);
                    if (getState().equals(STATE_RUNNING)) {
                        setState(STATE_FINISHED);
                    }
                }
            }
            if (getState().equals(STATE_STOPPING)) {
                setState(STATE_STOPPED);
            }
        } catch (DataTransferException ex) {
            setState(STATE_WRONG);
            getOutput().error(table.getName() + "出错！！", ex);
            getOutput().error(ex.getMessage(), ex);
            ex.printStackTrace();
        } catch (Exception ex) {
            setState(STATE_WRONG);
            ex.printStackTrace();
            getOutput().error(table.getName() + "出错！！", ex);
            getOutput().error(ex.getMessage(), ex);

        } finally {
            // System.out.println("连接池连接数量为："+ConnectionManager.getOpenConnectionCount(from));
            try {
                from_connection.close();
                to_connection.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        System.out.println(table.getName() + "结束");
        getOutput().prompt(table.getName() + ":" + getState());
    }

    /**
     * @return the count
     */
    public int getCount() {
        return count;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    /**
     * @param count
     *            the count to set
     */
    public void setCount(int count) {
        this.count = count;
        fireChange();
    }

    /**
     * @return the perCount
     */
    public int getPerCount() {
        return perCount;
    }

    /**
     * @param perCount
     *            the perCount to set
     */
    public void setPerCount(int perCount) {
        if (!state.equals(STATE_RUNNING)) {
            this.perCount = perCount;
            fireChange();
        }
        this.perCountNew = perCount;

    }

    /**
     * @return the name
     */
    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @param state
     *            the state to set
     */
    public void setState(String state) {
        String oldState = this.state;
        this.state = state;
        getOutput().info(
                getName() + "状态改变：from " + oldState + " to " + this.state);
        fireChange();
    }

    /**
     * @return the total
     */
    public int getTotal() {
        return total;
    }

    /**
     * @param total
     *            the total to set
     */
    public void setTotal(int total) {
        this.total = total;
        fireChange();
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public String getName() {
        return table.getName();
    }

    private String generateSelect(Table table) {
        Column[] columns = table.getColumns();
        String sql = "select ";
        for (int i = 0; i < columns.length; i++) {
            sql += columns[i].getName() + ",";
        }
        int length = sql.length();
        if (null != from.getSchema() && from.getSchema().length() > 0) {
            sql = sql.substring(0, length - 1) + " from " + from.getSchema()
                    + "." + table.getName();
        } else {
            sql = sql.substring(0, length - 1) + " from " + table.getName();
        }
        return sql;
    }

    private String generateSelect(Table table, String whereCondtion) {
        Column[] columns = table.getColumns();
        String sql = "select ";
        for (int i = 0; i < columns.length; i++) {
            sql += columns[i].getName() + ",";
        }
        int length = sql.length();
        if (null != from.getSchema() && from.getSchema().length() > 0) {
            sql = sql.substring(0, length - 1) + " from " + from.getSchema()
                    + "." + table.getName();
        } else {
            sql = sql.substring(0, length - 1) + " from " + table.getName();
        }
        if (null != whereCondtion && whereCondtion.trim().length() > 0) {
            if (whereCondtion.trim().startsWith("AND")) {
                whereCondtion = whereCondtion.replaceFirst("AND", "");
            }
            sql = sql + "  where " + whereCondtion;
        }
        return sql;
    }

    public String getGeneratedSelect() {
        Column[] columns = table.getColumns();
        String sql = "select ";
        for (int i = 0; i < columns.length; i++) {
            sql += columns[i].getName() + ",";
        }
        int length = sql.length();
        if (null != from.getSchema() && from.getSchema().length() > 0) {
            sql = sql.substring(0, length - 1) + " from " + from.getSchema()
                    + "." + table.getName();
        } else {
            sql = sql.substring(0, length - 1) + " from " + table.getName();
        }
        if (null != whereCondition && whereCondition.trim().length() > 0) {
            if (whereCondition.trim().startsWith("AND")) {
                whereCondition = whereCondition.replaceFirst("AND", "");
            }
            sql = sql + "  where " + whereCondition;
        }
        return sql;
    }

    private String generateCountSelect(Table table, String whereCondtion) {
        Column[] columns = table.getColumns();
        String sql = "select count(*) ";
        int length = sql.length();
        if (null != from.getSchema() && from.getSchema().length() > 0) {
            sql = sql.substring(0, length - 1) + " from " + from.getSchema()
                    + "." + table.getName();
        } else {
            sql = sql.substring(0, length - 1) + " from " + table.getName();
        }
        if (null != whereCondtion && whereCondtion.trim().length() > 0) {
            if (whereCondtion.trim().startsWith("AND")) {
                whereCondtion = whereCondtion.replaceFirst("AND", "");
            }
            sql = sql + "  where " + whereCondtion;
        }
        System.out.println("计算合计的sql" + sql);
        return sql;
    }

    public String getUserExtractSql() {
        String sql = RepositoryCache.getCurrentConfig().getUserExtractSql(fromDatabase.getName(), table.getName());
        if (null == sql || sql.trim().length() == 0) {
            sql = getGeneratedSelect();
            setUserExtractSql(sql);
        }
        return sql;
    }

    public void setUserExtractSql(String sql) {
        DataTransferConfig dtc = RepositoryCache.getCurrentConfig();
        userExtractSql = new UserExtractSql(fromDatabase.getName(),table.getName(),sql);
        dtc.putUserExtractSql(userExtractSql);
        RepositoryCache.writeDataTransferConfig(dtc);
    }

    private String getUserExtractSqlCount() {
        Column[] columns = table.getColumns();
        String sql = "select count(*) ";
        String sql1 = getUserExtractSql();
        int i = sql1.toUpperCase().indexOf(" FROM ");
        sql = sql + sql1.substring(i);
        return sql;
    }

    /**
     * 剩余行数
     * 
     * @return
     */
    public int getRemain() {
        return total - count;
    }

    /**
     * 完成百分比
     * 
     * @return
     */
    public String getPercent() {
        if (getTotal() != 0 && getCount() < getTotal()) {
            double percent = getCount() * 100.0 / getTotal();
            return new java.text.DecimalFormat("#.00").format(percent) + "%";
        } else {
            return "100%";
        }
    }

    private void calcuteTotal() throws DataTransferException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            conn = ConnectionManager.getConnection(from);
            String sql = this.getUserExtractSqlCount();

            st = conn.prepareStatement(sql);
            rs = st.executeQuery();
            if (rs.next()) {
                setTotal(rs.getInt(1));
            }
            System.out.println(total);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new DataTransferException("获取总记录数出错，datasource:"
                    + from.getName() + ",table:" + table.getName(), ex);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != st) {
                    st.close();
                }
                conn.close();
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public String toString() {
        return "数据抽取线程，表：" + getName();
    }

    public void stop() {
        if (!getState().equals(STATE_FINISHED)) {
            setState(STATE_STOPPING);
        }
    }

    private void fireChange() {
        for (TaskListener tl : taskListenerList) {
            tl.change();
        }
    }

    public void registerTaskListener(TaskListener taskListener) {
        taskListenerList.add(taskListener);
    }

    public void removeTaskListener(TaskListener taskListener) {
        taskListenerList.remove(taskListener);
    }

    public DatabaseConnection getFrom() {
        return from;
    }

    public void setFrom(DatabaseConnection from) {
        this.from = from;
    }

    public DatabaseConnection getTo() {
        return to;
    }

    public void setTo(DatabaseConnection to) {
        this.to = to;
    }

    /**
     * @return the database
     */
    /**
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    public Database getToDatabase() {
        return toDatabase;
    }

    public void setToDatabase(Database toDatabase) {
        this.toDatabase = toDatabase;
    }

    public Database getFromDatabase() {
        return fromDatabase;
    }

    public void setFromDatabase(Database fromDatabase) {
        this.fromDatabase = fromDatabase;
    }

    /**
     * @param table
     *            the table to set
     */
    public void setTable(Table table) {
        this.table = table;
    }
}
