/*
 * 
 * 
 */
package datatransfer.config;

import java.util.List;

import pl.mpak.util.id.UniqueID;

/**
 *
 * @author zgw@dongying.pbc
 */
public class TransferQuery {
    private String sql=" ";
    private String destinationTable;
    private String sequncedAlias;
    private Boolean isAliasValid;
    
    public TransferQuery()  {
    }

     

	public Boolean getIsAliasValid() {
		return isAliasValid;
	}



	public void setIsAliasValid(Boolean isAliasValid) {
		this.isAliasValid = isAliasValid;
	}



	public String getSequncedAlias() {
		return sequncedAlias;
	}
 

	public void setSequncedAlias(String sequncedAlias) {
		this.sequncedAlias = sequncedAlias;
	}

 

	/**
     * @return the sql
     */
    public String getSql() {
        return sql;
    }
    public String getInsertsql() {
    	StringBuffer s=new StringBuffer("insert into " +destinationTable);
    	s.append(getSequncedAlias().replace("[", "(").replace("]",")"));
    	s.append(getSelSql());
        return s.toString();
    }
    public String getSelSql()
    {
    	return sql.replaceAll("\\[[^]]*\\]", "");
    }
    /**
     * @param sql the sql to set
     */
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * @return the destinationTable
     */
    public String getDestinationTable() {
        return destinationTable;
    }

    /**
     * @param destinationTable the destinationTable to set
     */
    public void setDestinationTable(String destinationTable) {
        this.destinationTable = destinationTable;
    }
    
    
}
