/*
 * 
 * 
 */
package datatransfer.extract;

import datatransfer.Output;
import datatransfer.config.TransferQuery;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;
import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.datasource.ConnectionManager;
import org.executequery.datasource.PooledConnection;
import org.executequery.repository.RepositoryCache;

/**
 *
 * @author zgw@dongying.pbc
 */
public class QueryExecuteTask implements Runnable {

    public static final String STATE_READY = "就绪";
    public static final String STATE_RUNNING = "运行";
    public static final String STATE_ERROR = "出错终止";
    public static final String STATE_FINISHED = "完成";
    public static final String STATE_STOPPED = "用户终止";
    private TransferQuery query;
    private Output output;
    private Connection conn;
    private String state;
    private List<TaskListener> taskListenerList;

    public QueryExecuteTask(TransferQuery query) {
        this.query = query;
        this.setState(STATE_READY);
        this.taskListenerList = new ArrayList<TaskListener>();
    }

    public void run() {
        if (state.equals(STATE_STOPPED)) {
            return;
        }
        setState(STATE_RUNNING);
        Statement stmt = null;
        DatabaseConnection to = RepositoryCache.getCurrentConfig().getDestination();
        try {
            conn = ConnectionManager.getConnection(to);
            stmt = conn.createStatement();
            output.info(getQuery().getDestinationTable() + ":开始清空数据");
            String del_str = null;
            if (null != to.getSchema()) {
                del_str = "delete from " + to.getSchema() + "." + getQuery().getDestinationTable();
            } else {
                del_str = "delete from " + getQuery().getDestinationTable();
            }
            stmt.execute(del_str);
            output.info(getQuery().getDestinationTable() + ":删除数据完成:" + stmt.getUpdateCount() + "条 ");
            String sql = getQuery().getInsertsql();
            output.info(getQuery().getDestinationTable() + ":开始执行，sql：" + sql);
            System.out.println((getQuery().getDestinationTable() + ":开始执行，sql：" + sql));
            stmt.execute(sql);
            output.info(getQuery().getDestinationTable() + ":插入数据完成:" + stmt.getUpdateCount() + "条 ");

            // conn.commit();
        } catch (SQLException ex) {
            ex.printStackTrace();
            output.error("执行出错--------", ex);
            output.error(ex.getMessage(), ex);
            output.error("---------------------", ex);
            if (!state.equals(STATE_STOPPED)) {
                this.setState(STATE_ERROR);
            }
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
                System.out.println("sql语句执行完毕！");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if (!getState().equals(STATE_ERROR) && !getState().equals(STATE_STOPPED)) {
                setState(STATE_FINISHED);
            }
        }
    }

    public void stop() {
        if (!getState().equals(STATE_RUNNING)) {
            output.info("无效终止命令，线程未处于执行状态"+this.getQuery().getDestinationTable());
        }else{
            this.setState(STATE_STOPPED);
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @param state the state to set
     */
    public void setState(String state) {
        if (output != null) {
            output.prompt(getQuery().getDestinationTable() + "状态改变:" + this.state + "-->" + state);
        }        
        this.state = state;
        this.fireChange();
    }

    private void fireChange() {
        if(taskListenerList!=null){
            for (TaskListener tl : taskListenerList) {
                tl.change();
            }
        }
    }

    public void registerTaskListener(TaskListener taskListener) {
        taskListenerList.add(taskListener);
    }

    public void removeTaskListener(TaskListener taskListener) {
        taskListenerList.remove(taskListener);
    }

    /**
     * @return the query
     */
    public TransferQuery getQuery() {
        return query;
    }

    /**
     * @param query the query to set
     */
    public void setQuery(TransferQuery query) {
        this.query = query;
    }
}
