package datatransfer.gui;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.BevelBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;

import org.executequery.repository.RepositoryCache;

import pl.mpak.sky.gui.swing.MessageBox;
import pl.mpak.sky.gui.swing.SwingUtil;
import datatransfer.config.ConfigIO;
import datatransfer.config.TransferQuery;

public class SelectTranferQueryDialog extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private JTable table;
	private pl.mpak.sky.gui.swing.Action cmClose;
	private pl.mpak.sky.gui.swing.Action cmOk;
	private pl.mpak.sky.gui.swing.Action cmDel;
	private Boolean exit=false;
	

	public static String   showDialog() {
				SelectTranferQueryDialog dialog = new SelectTranferQueryDialog();
				dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
				dialog.setModal(true);
				dialog.setVisible(true);

		 return  dialog.getData();
	}

	/**
	 * Create the dialog.
	 */
	public SelectTranferQueryDialog() {
		setBounds(100, 100, 543, 330);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.setViewportBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		contentPanel.add(scrollPane,BorderLayout.CENTER);
		table = new JTable();
		table.setModel(new DefaultTableModel(
			new Object[][] {
				{null, null},
				{null, null},
				{null, null},
			},
			new String[] {
				"目标表名称", "转换语句"
			}
		));
		scrollPane.setViewportView(table);
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			JButton okButton = new JButton("OK");
			okButton.setActionCommand("OK");
			buttonPane.add(okButton);
			getRootPane().setDefaultButton(okButton);
			JButton cancelButton = new JButton("Cancel");
			cancelButton.setActionCommand("Cancel");
			buttonPane.add(cancelButton);
			JButton delButton = new JButton("Delete");
			cancelButton.setActionCommand("Delete");
			buttonPane.add(delButton);
		 cmClose = new pl.mpak.sky.gui.swing.Action();
		 cmClose.setShortCut(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ESCAPE, 0));
	     cmClose.setText("取消"); // NOI18N
	     cmClose.addActionListener(new java.awt.event.ActionListener() {
	            public void actionPerformed(java.awt.event.ActionEvent evt) {
	                cmCloseActionPerformed(evt);
	            }
	        });
	     cmOk = new pl.mpak.sky.gui.swing.Action();
	     cmOk.setShortCut(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ESCAPE, 0));
	     cmOk.setText("加载"); // NOI18N
	     cmOk.addActionListener(new java.awt.event.ActionListener() {
	            public void actionPerformed(java.awt.event.ActionEvent evt) {
	                cmOkActionPerformed(evt);
	            }
	        });
	     cmDel = new pl.mpak.sky.gui.swing.Action();
	     cmDel.setShortCut(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ESCAPE, 0));
	     cmDel.setText("删除"); // NOI18N
	     cmDel.addActionListener(new java.awt.event.ActionListener() {
	            public void actionPerformed(java.awt.event.ActionEvent evt) {
	                cmDelActionPerformed(evt);
	            }
	        }); 
	    delButton.setAction(cmDel);
	    okButton.setAction(cmOk);
	    cancelButton.setAction(cmClose);
		SwingUtil.centerWithinScreen(this);
	    loadTable();
	}

	private void loadTable() {
		DefaultTableModel tableModel = (DefaultTableModel) table.getModel();
		tableModel.setRowCount(0);// 清除原有行
		// 填充数据
		for (TransferQuery transferQuery : RepositoryCache.getCurrentConfig()
				.getTransferQuerys()) {
			String[] arr = new String[3];
			arr[0] = transferQuery.getDestinationTable();
			if(transferQuery.getSql().length()>50)
			arr[1] = transferQuery.getSql().substring(0, 50) + "..";
			else
		    arr[1] = transferQuery.getSql();
			// 添加数据到表格
			tableModel.addRow(arr);
		}
	}
	private String getData() {
		String  arr="";
		if(table.getSelectedRow()>-1)
		{
			arr=(String)table.getValueAt(table.getSelectedRow(), 0);
		}
		if(exit==false)
		return arr;	
		else
			return "";
	}
	private void cmCloseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmCloseActionPerformed
		exit=true;
		dispose();
	}
	private void cmOkActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmCloseActionPerformed
	     
		if(this.table.getSelectedRow()>-1)
		{
			exit=false;
			dispose();
		}
		else
		{
			exit=true;
		}
			
	}
	private void cmDelActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_cmCloseActionPerformed
	     
		if(this.table.getSelectedRow()>-1)
		{
			RepositoryCache.getCurrentConfig()
			.getAllTransferQuerys().remove(table.getModel().getValueAt(table.getSelectedRow(), 0));
			 loadTable();
			 writeConfig();
		}
		else
		{
			MessageBox.show("提示", "没有选中的条目");
		}
			
	}
	private void writeConfig()
	{
		File file = new File(RepositoryCache.getCurrentConfig().getConfigFile());
        ConfigIO cio = new ConfigIO();
        FileWriter outputWriter = null;
        try {
        //    if (file.exists()) {
              //  if (MessageBox.show(this, "提示", "配置文件已存在，要覆盖它么？", ModalResult.YESNO, MessageBox.QUESTION) != ModalResult.YES)
             //       return;
        //    } 
       // else {
                File dir = file.getParentFile();
                boolean dirOk = true;
                if(!dir.exists()){                    
                    dirOk = dir.mkdirs();
                }
                if(dirOk){
                    file.createNewFile();
                }else{
                    MessageBox.show("提示", "保存配置文件失败，不能创建所在目录");
                    return;
                }
         //   }            
            outputWriter = new FileWriter(file);
            cio.write(RepositoryCache.getCurrentConfig(), outputWriter);
            MessageBox.show("提示", "配置文件已保存："+file.getAbsolutePath());
            RepositoryCache.getCurrentConfig().setChangedAndUnsaved(false);
        } catch (Exception ex) {
            MessageBox.show("提示", "保存配置文件失败:"+ex.getMessage());
        } finally{
            if(outputWriter!=null){
                try {
                    outputWriter.close();
                } catch (IOException ex) {
                 //   logger.error(ex);
                }
            }
           }
	}
		
	
	private DefaultTableModel loadTable1() {
		Vector<String> tableHeaders = new Vector<String>();
		tableHeaders.add("目标表名称");
		tableHeaders.add("转换脚本语句");
		Vector tableData = new Vector();
		// oneRow.add(tab.getId());
		for (TransferQuery transferQuery : RepositoryCache.getCurrentConfig()
				.getTransferQuerys()) {
			Vector oneRow = new Vector();
			oneRow.add(transferQuery.getDestinationTable());
			oneRow.add(transferQuery.getSql().substring(0, 20) + "..");
			tableData.add(oneRow);
		}
		return new DefaultTableModel(tableData, tableHeaders);
	}
}
