/*
 * 
 * 
 */
package datatransfer.config;

/**
 *
 * @author zgw@dongying.pbc
 */
public class UserExtractSql{
    private String database;
    private String table;
    private String sql;
    
    public UserExtractSql(){
        
    }
    
    public UserExtractSql(String database,String table,String sql){
        this.database = database;
        this.table = table;
        this.sql = sql;
    }

    /**
     * @return the datasource
     */
    public String getDatabase() {
        return database;
    }

    /**
     * @param datasource the datasource to set
     */
    public void setDatabase(String datasource) {
        this.database = datasource;
    }

    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * @return the sql
     */
    public String getSql() {
        return sql;
    }

    /**
     * @param sql the sql to set
     */
    public void setSql(String sql) {
        this.sql = sql;
    }
    
}
