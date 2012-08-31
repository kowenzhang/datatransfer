/*
 * 
 * 
 */
package datatransfer.gui.extract;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import datatransfer.extract.DataExtractTask;
import datatransfer.extract.TaskListener;
import javax.swing.table.AbstractTableModel;

import org.executequery.databaseobjects.impl.ColumnConstraint;
import org.executequery.databaseobjects.impl.DefaultDatabaseColumn;

/**
 *
 * @author zgw@dongying.pbc
 */
public class TaskTableModel extends AbstractTableModel implements TaskListener{

    private DataExtractTask[] tasks;
    private String[] columnNames = {"表名","状态","待完成行数","已完成行数","进度","缓存","分析脚本","执行脚本"};   

    public TaskTableModel(DataExtractTask[] tasks) {
        this.tasks = tasks;
        for (DataExtractTask task : tasks) {
            task.registerTaskListener(this);
        }        
    }

    public int getRowCount() {
        return tasks.length;
    }

    public int getColumnCount() {
        return 8;
    }
    
    public String getColumnName(int columnIndex){
        return columnNames[columnIndex];
    }
   

    public Object getValueAt(int rowIndex, int columnIndex) {
        DataExtractTask task = tasks[rowIndex];
        Object value = null;
        switch (columnIndex) {
            case 0:
                value = task.getName();
                break;
            case 1:
                value = task.getState();
                break;
            case 2:
                value = task.getRemain();              
                break;
            case 3:
                value = task.getCount();
                break;
            case 4:
                value = task.getPercent();
                break;
            case 5:
                value = task.getPerCount();
                break;
            case 6:
                value = task.getGeneratedSelect();
                break;
            case 7:
                value = task.getUserExtractSql();
                break;
        }
        return value;
    }
    public void setValueAt(Object value, int row, int col) {
        DataExtractTask task = tasks[row];
         
        switch (col) {
            case 7:
              task.setUserExtractSql(value.toString());
              break;
        }
        fireTableRowsUpdated(row, row);
         
    }
    public void change() {
        fireTableDataChanged();
    }
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        
          if (columnIndex >6)  
                return true;
          else return false;
    }

	 
     
    
}
