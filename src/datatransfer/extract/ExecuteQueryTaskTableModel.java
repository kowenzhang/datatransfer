/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datatransfer.extract;

import javax.swing.table.AbstractTableModel;

/**
 *
 * @author kowen
 */
public class ExecuteQueryTaskTableModel  extends AbstractTableModel implements TaskListener{

    private QueryExecuteTask[] tasks;
    private String[] columnNames = {"目标表","状态","执行脚本"};   
    

    public ExecuteQueryTaskTableModel(QueryExecuteTask[] tasks) {
        this.tasks = tasks;
        for (QueryExecuteTask task : tasks) {
            task.registerTaskListener(this);
        }        
    }

    public int getRowCount() {
        return tasks.length;
    }

    public int getColumnCount() {
        return 3;
    }
    
    public String getColumnName(int columnIndex){
        return columnNames[columnIndex];
    }
   

    public Object getValueAt(int rowIndex, int columnIndex) {
        QueryExecuteTask task = tasks[rowIndex];
        Object value = null;
        switch (columnIndex) {
            case 0:
                value = task.getQuery().getDestinationTable();
                break;
            case 1:
                value = task.getState();
                break;
            case 2:
                value = task.getQuery().getInsertsql();  
                break;
        }
        return value;
    }
    
    public void change() {
        fireTableDataChanged();
    }         
    
}