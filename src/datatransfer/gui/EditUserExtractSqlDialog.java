package datatransfer.gui;

import javax.swing.JDialog;
import javax.swing.JTextField;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import javax.swing.JTextPane;
import javax.swing.JTextArea;

public class EditUserExtractSqlDialog extends JDialog {
	private JTextArea textArea;
	
	public EditUserExtractSqlDialog(String userExtractSql) {
		setBounds(100, 100, 342, 145);
		getContentPane().setLayout(new BorderLayout());
		textArea = new JTextArea();
		textArea.setLineWrap(true);
		textArea.setText(userExtractSql);
		getContentPane().add(textArea, BorderLayout.CENTER);
	}

	public static String   showDialog(String userExtractSql) {
		EditUserExtractSqlDialog dialog = new EditUserExtractSqlDialog(userExtractSql);
		dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		dialog.setModal(true);
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);

     return  dialog.getData();
}
	
	private String getData() {
		 
			return textArea.getText();
	}
	
	
}
