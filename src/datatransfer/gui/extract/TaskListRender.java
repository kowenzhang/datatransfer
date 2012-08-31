/*
 * 
 * 
 */
package datatransfer.gui.extract;

import datatransfer.extract.DataExtractTask;
import java.awt.Component;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JList;

/**
 *
 * @author zgw@dongying.pbc
 */
public class TaskListRender extends DefaultListCellRenderer {

    @Override
    public Component getListCellRendererComponent(JList list, Object value, int index,
            boolean isSelected, boolean cellHasFocus) {
        super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
        if(value instanceof DataExtractTask){
            setText(((DataExtractTask) value).getName());
        }else if (value!=null){
            setText(value.toString());
        }else{
            setText("");
        }
        return this;
    }
}
