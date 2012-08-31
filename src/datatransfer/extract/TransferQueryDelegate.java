package datatransfer.extract;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.gui.editor.QueryEditor;
import org.executequery.sql.QueryDelegate;
import org.executequery.sql.QueryDispatcher;

public class TransferQueryDelegate implements QueryDelegate {
	private int currentStatementHistoryIndex = -1;
	private final QueryDispatcher dispatcher;

	  public TransferQueryDelegate(QueryEditor queryEditor) {

	        super();
	        dispatcher = new QueryDispatcher(this);
	    }
	@Override
	public void commitModeChanged(boolean autoCommit) {
		// TODO Auto-generated method stub

	}

	@Override
	public void executing() {
		// TODO Auto-generated method stub
	}

	@Override
	public void setResult(int result, int type) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStatusMessage(String text) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOutputMessage(int type, String text) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOutputMessage(int type, String text, boolean selectTab) {
		// TODO Auto-generated method stub

	}

	@Override
	public void interrupt() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setResultSet(ResultSet rs, String query) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void statementExecuted(String statement) {
		// TODO Auto-generated method stub

	}

	@Override
	public void finished(String message) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isLogEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void log(String message) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		executeQuery("commit");
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		executeQuery("rollback");
	}

	@Override
	public void executeQuery(String query) {
		// TODO Auto-generated method stub
		// executeQuery( databaseconnection, query, false);
	}
	public void executeQuery(DatabaseConnection selectedConnection,
            String query, boolean executeAsBlock) {

        if (dispatcher.isExecuting()) {

            return;
        }

        if (query == null) {

           // query = queryEditor.getEditorText();
        }

        if (StringUtils.isNotBlank(query)) {

            currentStatementHistoryIndex = -1;
          //  queryEditor.setHasPreviousStatement(true);
          //  queryEditor.setHasNextStatement(false);
            dispatcher.executeSQLQuery(selectedConnection, query, executeAsBlock);
        }

    }
	@Override
	public void executeQuery(String query, boolean executeAsBlock) {
		// TODO Auto-generated method stub

	}

}
