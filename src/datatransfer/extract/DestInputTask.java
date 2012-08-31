/*
 * 
 * 
 */
package datatransfer.extract;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.executequery.databasemediators.QueryTypes;
import org.executequery.datasource.ConnectionManager;
import org.executequery.datasource.PooledConnection;
import org.executequery.log.Log;
import org.executequery.repository.RepositoryCache;
import org.executequery.sql.DerivedQuery;
import org.executequery.sql.QueryTokenizer;
import org.executequery.sql.SqlMessages;
import org.executequery.util.UserProperties;

import datatransfer.Output;

/**
 *
 * @author zgw@dongying.pbc
 */
public class DestInputTask implements Runnable {
    private static final String SUBSTRING = "...";
    private static final String EXECUTING_1 = "Executing: ";
    private static final String ERROR_EXECUTING = " Error executing statement";
    private static final String DONE = " Done";
    private static final String COMMITTING_LAST = "Committing last transaction block...";
    private static final String ROLLING_BACK_LAST = "Rolling back last transaction block...";

    public static final String STATE_FINISHED = "完成";
    public static final String STATE_RUNNING = "正在运行";
    public static final String STATE_READY = "就绪";
    public static final String STATE_WRONG = "出错停止";
    public static final String STATE_STOPPED = "用户终止";
    private static final String STATE_STOPPING = "正在终止";

    /** indicates verbose logging output */
    private boolean verboseLogging;

    /** Indicates that an execute is in progress */
    private boolean executing;

    /** connection commit mode */
    private boolean autoCommit;

    /** The query execute duration time */
    private String duration;

    /** indicates that the current execution has been cancelled */
    private boolean statementCancelled;
    private QueryTokenizer queryTokenizer;
    private String sql;
    private String tableName;
    private int total;
    private int count = 0;
    private int perCount = 100;
    private int perCountNew = 100;
    private String state;
    private List<TaskListener> taskListenerList;
    private Output output;
    private byte[] lockOfPerCount = new byte[0];
    private Connection conn;

    public DestInputTask(String sql,String tableName) {
        this.sql=sql;
        this.tableName=tableName;
        conn=ConnectionManager.getConnection(RepositoryCache.getCurrentConfig().getDestination());
        taskListenerList = new ArrayList<TaskListener>();
        this.state = STATE_READY;
    }
    public void reInit()
    {
    	
    }

    public void run() {
        if (getState().equals(STATE_READY)) {
            this.setState(STATE_RUNNING);
        }
        try {
     
            //calcuteTotal();
            System.out.println("开始写入表 :"+tableName);
            getOutput().info("开始写入表 :"+tableName);
            System.out.println(executeSQL(sql));
            System.out.println("完成写入表:"+tableName);
            getOutput().info("完成写入表:"+tableName);
            if (getState().equals(STATE_STOPPING)) {
                setState(STATE_STOPPED);
            ((PooledConnection)conn).setInUse(false);
            }
         
        } catch (Exception ex) {
            setState(STATE_WRONG);
            getOutput().error("出错！！", ex);
         ex.printStackTrace() ;
        }
       
    }
   

    
 
    public void preferencesChanged() {

        initialiseLogging();
    }

    private UserProperties userProperties() {

        return UserProperties.getInstance();
    }

    private void initialiseLogging() {

        verboseLogging = userProperties().getBooleanProperty("editor.logging.verbose");

        newLineMatcher = Pattern.compile("\n").matcher("");
    }

   

    /**
     * Indicates a connection has been closed.
     * Propagates the call to the query sender object.
     *
     * @param the connection thats been closed
     */
    
    /**
     * Returns the current commit mode.
     *
     * @return the commit mode
     */
    public boolean getCommitMode() {
        return autoCommit;
    }

    
    
    
    

    /**
     * Returns whether a a query is currently being executed.
     *
     * @param true if in an execution is in progress, false otherwise
     */
    public boolean isExecuting() {

        return executing;
    }
    
     private Object executeSQL(String sql)  {
        
    	Statement  st = null;
        ResultSet rs = null;
        try {
        	st=conn.createStatement();
        	System.out.println("开始执行语句 :"+sql);
        	getOutput().info("开始执行语句 :"+sql);
            int total=st.executeUpdate(sql);
             
            getOutput().info("表"+tableName+"导入完毕，工导入数据条目条数："+total);
            return DONE;
        }   catch (SQLException e) {
        	 getOutput().info(tableName+"写入时发生查询错误");
            e.printStackTrace();
            return "SQLException1";

        }  catch (OutOfMemoryError e) {

            setOutputMessage(SqlMessages.ERROR_MESSAGE,
                    "Resources exhausted while executing query.\n"+
                    "The query result set was too large to return.");
            getOutput().info(tableName+"写入时发生内存溢出错误");	
            setStatusMessage(ERROR_EXECUTING);
            return "SQLException2";
        } catch (Exception e) {
        	getOutput().info(tableName+"写入时发生其他错误");
        	e.printStackTrace();

            if (!statementCancelled) {

                if (Log.isDebugEnabled()) {

                    e.printStackTrace();
                }

                processException(e);
            }
            return "SQLException3";

        }finally {
            try {
 
            	
            	if(st!=null)
                st.close();
            //	if(conn!=null)
             //   conn.close();
            } catch (SQLException e) {
            }
        }
    }
    

     
     

    

    /**
     * Logs the execution duration within the output
     * pane for the specified start and end values.
     *
     * @param start the start time in millis
     * @param end the end time in millis
     */
    

     
    private void logExecution(String query) {

        Log.info(EXECUTING_1 + query);

        if (verboseLogging) {

            setOutputMessage(
                    SqlMessages.ACTION_MESSAGE, EXECUTING_1);
            setOutputMessage(
                    SqlMessages.ACTION_MESSAGE_PREFORMAT, query);

        } else {

            int queryLength = query.length();
            int subIndex = queryLength < 50 ? (queryLength + 1) : 50;

            setOutputMessage(
                    SqlMessages.ACTION_MESSAGE, EXECUTING_1);
            setOutputMessage(
                    SqlMessages.ACTION_MESSAGE_PREFORMAT,
                    query.substring(0, subIndex-1).trim() + SUBSTRING);
        }

    }

    private void processException(Throwable e) {

        if (e != null) {
            setOutputMessage(SqlMessages.ERROR_MESSAGE, e.getMessage());

            if (e instanceof SQLException) {

                SQLException sqlExc = (SQLException)e;
                sqlExc = sqlExc.getNextException();

                if (sqlExc != null) {

                    setOutputMessage(SqlMessages.ERROR_MESSAGE, sqlExc.getMessage());
                }

            } else {

                setStatusMessage(ERROR_EXECUTING);
            }
        }

    }

    

    private void setStatusMessage(final String text) {
    	getOutput().info(text);
    }

    private void setOutputMessage(final int type, final String text) {
    	getOutput().info(text);
    //    setOutputMessage(type, text, true);
    }

 

  
    /** matcher to remove new lines from log messages */
    private Matcher newLineMatcher;

    /**
     * Logs the specified text to the logger.
     *
     * @param text - the text to log
     */

 
    /**
     * Dtermines whether the specified query is attempting
     * to create a SQL PROCEDURE or FUNCTION.
     *
     * @param query - the query to be executed
     * @return true | false
     */
    private boolean isCreateProcedureOrFunction(String query) {

        String noCommentsQuery = queryTokenizer.removeComments(query);
        if (isNotSingleStatementExecution(noCommentsQuery)) {

            return isCreateProcedure(noCommentsQuery) || isCreateFunction(noCommentsQuery);
        }

        return false;
    }

    /**
     * Dtermines whether the specified query is attempting
     * to create a SQL PROCEDURE.
     *
     * @param query - the query to be executed
     * @return true | false
     */
    private boolean isCreateProcedure(String query) {

        int createIndex = query.indexOf("CREATE");
        int tableIndex = query.indexOf("TABLE");
        int procedureIndex = query.indexOf("PROCEDURE");
        int packageIndex = query.indexOf("PACKAGE");

        return (createIndex != -1) && (tableIndex == -1) &&
                (procedureIndex > createIndex || packageIndex > createIndex);
    }

    /**
     * Determines whether the specified query is attempting
     * to create a SQL FUNCTION.
     *
     * @param query - the query to be executed
     * @return true | false
     */
    private boolean isCreateFunction(String query) {
        int createIndex = query.indexOf("CREATE");
        int tableIndex = query.indexOf("TABLE");
        int functionIndex = query.indexOf("FUNCTION");
        return createIndex != -1 &&
               tableIndex == -1 &&
               functionIndex > createIndex;
    }

    private boolean isNotSingleStatementExecution(String query) {

        DerivedQuery derivedQuery = new DerivedQuery(query);
        int type = derivedQuery.getQueryType();

        int[] nonSingleStatementExecutionTypes = {
                QueryTypes.CREATE_FUNCTION,
                QueryTypes.CREATE_PROCEDURE,
                QueryTypes.UNKNOWN,
                QueryTypes.EXECUTE
        };

        for (int i = 0; i < nonSingleStatementExecutionTypes.length; i++) {

            if (type == nonSingleStatementExecutionTypes[i]) {

                return true;
            }

        }

        return false;
    }
    /**
     * @return the count
     */
    public int getCount() {
        return count;
    }

    /**
     * @param count the count to set
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
     * @param perCount the perCount to set
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
     * @param state the state to set
     */
    public void setState(String state) {
        String oldState = this.state;
        this.state = state;
       // getOutput().info(getName() + "状态改变：from " + oldState + " to " + this.state);
        fireChange();
    }

    /**
     * @return the total
     */
    public int getTotal() {
        return total;
    }

    /**
     * @param total the total to set
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

   
 

    /**
     * 剩余行数
     * @return 
     */
    public int getRemain() {
        return total - count;
    }

    /**
     * 完成百分比
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

    private void setResultSet(final ResultSet rs, final String query) {
         
    }
 

	 
}
