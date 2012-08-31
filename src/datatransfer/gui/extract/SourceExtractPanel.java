/*
 * 
 * 
 */

/*
 * SourceStatePanel.java
 *
 * Created on 2012-6-14, 15:52:43
 */
package datatransfer.gui.extract;

import datatransfer.config.TransferQuery;
import datatransfer.extract.DataExtractTask;
import datatransfer.extract.TaskListener;
import datatransfer.extract.ExtractTaskExecutor;
import datatransfer.gui.EditUserExtractSqlDialog;
import datatransfer.gui.SelectTranferQueryDialog;

import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.JTable;

import org.executequery.repository.RepositoryCache;

import pl.mpak.sky.gui.mr.ModalResult;
import pl.mpak.sky.gui.swing.MessageBox;
import pl.mpak.util.StringUtil;

/**
 * 
 * @author zgw@dongying.pbc
 */
public class SourceExtractPanel extends javax.swing.JPanel implements
        TaskListener, MouseListener {

    private ExtractTaskExecutor pool;
    private ExtractPanel extractPanel;

    /** Creates new form SourceStatePanel */
    public SourceExtractPanel() {
        initComponents();
        initTableModel();
//		setSplitDivision();
    }

    public SourceExtractPanel(List<DataExtractTask> taskList) {
        this(taskList, 10, 1000);
    }

    /** Creates new form SourceStatePanel */
    public SourceExtractPanel(List<DataExtractTask> taskList, int threadNum,
            int bufferSize) {
        initComponents();
//		setSplitDivision();
        this.taskList.addAll(taskList);
        setBufferSize(bufferSize);
        setThreadNum(threadNum);
        initTableModel();
    }

    private void initTableModel() {
        ExtractTaskTableModel tablemodel = new ExtractTaskTableModel(
                this.taskList.toArray(new DataExtractTask[taskList.size()]));
        this.jTable1.setModel(tablemodel);
        jTable1.addMouseListener(this);
    }

    @Override
    public void mouseClicked(MouseEvent e) {
        if (e.getClickCount() == 1) {
            // 单击
            JTable tb = (JTable) e.getSource();
            int row = tb.getSelectedRow();
            int col = tb.getSelectedColumn();
            if (col > 5) {
                ExtractTaskTableModel tablemodel = (ExtractTaskTableModel) tb.getModel();
                String userExtractSql = (String) tablemodel.getValueAt(row, col);
                userExtractSql = EditUserExtractSqlDialog.showDialog(userExtractSql);
                if (userExtractSql.length() > 0) {
                    tablemodel.setValueAt(userExtractSql, row, col);
                }



            }
        }
    }

    @Override
    public void mousePressed(MouseEvent e) {
        // TODO Auto-generated method stub
    }

    @Override
    public void mouseReleased(MouseEvent e) {
        // TODO Auto-generated method stub
    }

    @Override
    public void mouseEntered(MouseEvent e) {
        // TODO Auto-generated method stub
    }

    @Override
    public void mouseExited(MouseEvent e) {
        // TODO Auto-generated method stub
    }

    /**
     * 设置线程数
     * 
     * @param threadNum
     */
    public void setThreadNum(int threadNum) {
        if (threadNum > taskList.size()) {
            this.txtThreadNum.setText(Integer.toString(taskList.size()));
            this.txtThreadNum.setText(Integer.toString(taskList.size()));
        } else {
            this.txtThreadNum.setText(Integer.toString(threadNum));
            this.txtThreadNum.setText(Integer.toString(threadNum));
        }
    }

    public void setBufferSize(int bufferSize) {
        this.txtBuffer.setText(Integer.toString(bufferSize));
        for (DataExtractTask task : this.taskList) {
            task.setPerCount(bufferSize);
        }
    }

    /**
     * 创建线程开始执行任务
     */
    public void start() {
        if (StringUtil.isInteger(txtThreadNum.getText())) {
            int threadNum = Integer.parseInt(txtThreadNum.getText());
            setThreadNum(threadNum);

            for (DataExtractTask task : taskList) {
                task.setOutput(extractPanel.getOutput());
                task.registerTaskListener(this);
            }
            this.txtThreadNum.setEditable(false);
            this.btnStart.setEnabled(false);
            this.btnReset.setEnabled(false);
            this.btnStop.setEnabled(true);
            pool = new ExtractTaskExecutor(threadNum, taskList);
            pool.execute();

        }

    }

    public void change() {
        if (!hasRunning()) {// 没有任务在执行
            this.btnReset.setEnabled(true);
            this.btnStop.setEnabled(false);
            extractPanel.checkAllFinishAndStartExecute();
        }

    }

    public boolean hasRunning() {
        Iterator<DataExtractTask> it = taskList.iterator();
        while (it.hasNext()) {
            DataExtractTask task = it.next();
            if (task.getState().equals(DataExtractTask.STATE_RUNNING)) {
                return true;
            }
        }
        return false;
    }

    public boolean isFinished() {
        Iterator<DataExtractTask> it = taskList.iterator();
        while (it.hasNext()) {
            DataExtractTask task = it.next();
            if (!task.getState().equals(DataExtractTask.STATE_FINISHED)) {
                return false;
            }
        }
        return true;
    }

    public boolean canStart() {
        return btnStart.isEnabled();
    }

//	private void setSplitDivision() {
//		jSplitPane1
//				.setDividerLocation((int) ((double) (getHeight() - jSplitPane1
//						.getDividerSize()) * 0.7));
//		jSplitPane1.addComponentListener(new ComponentAdapter() {
//
//			public void componentResized(ComponentEvent e) {
//				jSplitPane1.setDividerLocation(0.7);
//			}
//		});
//	}
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed"
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {
        bindingGroup = new org.jdesktop.beansbinding.BindingGroup();

        taskList = new ArrayList<DataExtractTask>();
        group = new javax.swing.ButtonGroup();
        panelControl = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        txtBuffer = new javax.swing.JTextField();
        btnChangeBuffer = new javax.swing.JButton();
        btnReset = new javax.swing.JButton();
        btnStart = new javax.swing.JButton();
        txtThreadNum = new javax.swing.JTextField();
        jLabel2 = new javax.swing.JLabel();
        btnStop = new javax.swing.JButton();
        radioOne = new javax.swing.JRadioButton();
        radioAll = new javax.swing.JRadioButton();
        cmbTasks = new javax.swing.JComboBox();
        jLabel4 = new javax.swing.JLabel();
        jScrollPane1 = new javax.swing.JScrollPane();
        jTable1 = new javax.swing.JTable();

        setLayout(new java.awt.BorderLayout());

        panelControl.setName("panelControl"); // NOI18N
        panelControl.setPreferredSize(new java.awt.Dimension(750, 40));

        jLabel3.setFont(new java.awt.Font("宋体", 1, 12));
        jLabel3.setText("更改缓存");
        jLabel3.setName("jLabel3"); // NOI18N

        txtBuffer.setText("10");
        txtBuffer.setName("txtBuffer"); // NOI18N

        btnChangeBuffer.setText("修改");
        btnChangeBuffer.setName("btnChangeBuffer"); // NOI18N
        btnChangeBuffer.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnChangeBufferActionPerformed(evt);
            }
        });

        btnReset.setText("重置");
        btnReset.setEnabled(false);
        btnReset.setName("btnReset"); // NOI18N
        btnReset.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnResetActionPerformed(evt);
            }
        });

        btnStart.setText("抽取"); // NOI18N
        btnStart.setName("btnStart"); // NOI18N
        btnStart.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnStartActionPerformed(evt);
            }
        });

        txtThreadNum.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtThreadNum.setText("5");
        txtThreadNum.setName("txtThreadNum"); // NOI18N

        jLabel2.setText("线程数");
        jLabel2.setName("jLabel2"); // NOI18N

        btnStop.setText("终止");
        btnStop.setName("btnStop"); // NOI18N
        btnStop.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnStopActionPerformed(evt);
            }
        });

        group.add(radioOne);
        radioOne.setText("单个");
        radioOne.setName("radioOne"); // NOI18N

        group.add(radioAll);
        radioAll.setSelected(true);
        radioAll.setText("全部");
        radioAll.setName("radioAll"); // NOI18N
        radioAll.addItemListener(new java.awt.event.ItemListener() {
            public void itemStateChanged(java.awt.event.ItemEvent evt) {
                radioAllItemStateChanged(evt);
            }
        });
        radioAll.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                radioAllActionPerformed(evt);
            }
        });

        cmbTasks.setEnabled(false);
        cmbTasks.setName("cmbTasks"); // NOI18N
        cmbTasks.setRenderer(new TaskListRender());

        org.jdesktop.swingbinding.JComboBoxBinding jComboBoxBinding = org.jdesktop.swingbinding.SwingBindings.createJComboBoxBinding(org.jdesktop.beansbinding.AutoBinding.UpdateStrategy.READ_ONCE, taskList, cmbTasks);
        bindingGroup.addBinding(jComboBoxBinding);

        jLabel4.setFont(new java.awt.Font("宋体", 1, 12));
        jLabel4.setText("控制");
        jLabel4.setName("jLabel4"); // NOI18N

        javax.swing.GroupLayout panelControlLayout = new javax.swing.GroupLayout(panelControl);
        panelControl.setLayout(panelControlLayout);
        panelControlLayout.setHorizontalGroup(
            panelControlLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelControlLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel4)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jLabel2)
                .addGap(18, 18, 18)
                .addComponent(txtThreadNum, javax.swing.GroupLayout.PREFERRED_SIZE, 66, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addComponent(btnStart)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(btnStop)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(btnReset)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 53, Short.MAX_VALUE)
                .addComponent(jLabel3)
                .addGap(18, 18, 18)
                .addComponent(radioAll)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(radioOne)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(cmbTasks, javax.swing.GroupLayout.PREFERRED_SIZE, 80, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addComponent(txtBuffer, javax.swing.GroupLayout.PREFERRED_SIZE, 66, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(btnChangeBuffer)
                .addContainerGap())
        );
        panelControlLayout.setVerticalGroup(
            panelControlLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelControlLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelControlLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel4)
                    .addComponent(jLabel3)
                    .addComponent(radioAll)
                    .addComponent(radioOne)
                    .addComponent(cmbTasks, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnChangeBuffer)
                    .addComponent(txtBuffer, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel2)
                    .addComponent(txtThreadNum, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnStart)
                    .addComponent(btnStop)
                    .addComponent(btnReset))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        add(panelControl, java.awt.BorderLayout.NORTH);

        jScrollPane1.setMinimumSize(new java.awt.Dimension(540, 120));
        jScrollPane1.setName("jScrollPane1"); // NOI18N
        jScrollPane1.setPreferredSize(new java.awt.Dimension(452, 200));

        jTable1.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {

            }
        ));
        jTable1.setName("jTable1"); // NOI18N
        jTable1.setPreferredSize(new java.awt.Dimension(400, 300));
        jTable1.setRowSelectionAllowed(false);
        jTable1.getTableHeader().setReorderingAllowed(false);
        jScrollPane1.setViewportView(jTable1);

        add(jScrollPane1, java.awt.BorderLayout.CENTER);

        bindingGroup.bind();
    }// </editor-fold>//GEN-END:initComponents

    private void btnStartActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_btnStartActionPerformed
        start();
    }// GEN-LAST:event_btnStartActionPerformed

    private void btnChangeBufferActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_btnChangeBufferActionPerformed
        if (StringUtil.isInteger(txtBuffer.getText())) {
            int bufferSize = Integer.parseInt(txtBuffer.getText());
            if (radioAll.isSelected()) {
                for (DataExtractTask task : taskList) {
                    task.setPerCount(bufferSize);
                }
            } else {
                DataExtractTask task = (DataExtractTask) cmbTasks.getSelectedItem();
                task.setPerCount(bufferSize);
            }
        }

    }// GEN-LAST:event_btnChangeBufferActionPerformed

    private void radioAllActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_radioAllActionPerformed
    }// GEN-LAST:event_radioAllActionPerformed

    private void radioAllItemStateChanged(java.awt.event.ItemEvent evt) {// GEN-FIRST:event_radioAllItemStateChanged
        cmbTasks.setEnabled(!radioAll.isSelected());
    }// GEN-LAST:event_radioAllItemStateChanged

    private void btnStopActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_btnStopActionPerformed
        if (MessageBox.show(this, "提示", "确认终止抽取任务？", ModalResult.YESNO,
                MessageBox.QUESTION) == ModalResult.YES) {
            this.btnReset.setEnabled(true);
            pool.shutdown();
        }
    }// GEN-LAST:event_btnStopActionPerformed

    private void btnResetActionPerformed(java.awt.event.ActionEvent evt) {// GEN-FIRST:event_btnResetActionPerformed
        if (MessageBox.show(this, "提示", "重置将首先清除目标表所有数据，确认重置以下抽取任务？",
                ModalResult.YESNO, MessageBox.QUESTION) == ModalResult.YES) {
            for (DataExtractTask task : taskList) {
                task.reInit();
            }
            this.txtThreadNum.setEditable(true);
            this.btnStart.setEnabled(true);
            this.txtThreadNum.setEditable(true);
        }
    }// GEN-LAST:event_btnResetActionPerformed
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnChangeBuffer;
    private javax.swing.JButton btnReset;
    private javax.swing.JButton btnStart;
    private javax.swing.JButton btnStop;
    private javax.swing.JComboBox cmbTasks;
    private javax.swing.ButtonGroup group;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTable jTable1;
    private javax.swing.JPanel panelControl;
    private javax.swing.JRadioButton radioAll;
    private javax.swing.JRadioButton radioOne;
    private java.util.List<datatransfer.extract.DataExtractTask> taskList;
    private javax.swing.JTextField txtBuffer;
    private javax.swing.JTextField txtThreadNum;
    private org.jdesktop.beansbinding.BindingGroup bindingGroup;
    // End of variables declaration//GEN-END:variables

    /**
     * @return the extractPanel
     */
    public ExtractPanel getExtractPanel() {
        return extractPanel;
    }

    /**
     * @param extractPanel
     *            the extractPanel to set
     */
    public void setExtractPanel(ExtractPanel extractPanel) {
        this.extractPanel = extractPanel;
    }
}
