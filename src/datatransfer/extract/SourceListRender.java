/*
 * 
 * 
 */
package datatransfer.extract;

import java.awt.Component;

import javax.swing.DefaultListCellRenderer;
import javax.swing.JList;

import org.executequery.databasemediators.DatabaseConnection;

/**
 *
 * @author zgw@dongying.pbc
 */
public class SourceListRender extends DefaultListCellRenderer {

    @Override
    public Component getListCellRendererComponent(JList list, Object value, int index,
            boolean isSelected, boolean cellHasFocus) {
        super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
        if(value instanceof DatabaseConnection){
            setText(((DatabaseConnection) value).getName());
        }else if (value!=null){
            setText(value.toString());
        }else{
            setText("");
        }
        return this;
    }
}
