/*
 * 
 * 
 */

/*
 * OutputPanel.java
 *
 * Created on 2012-6-16, 22:32:17
 */
package datatransfer.gui.swing;

import datatransfer.Output;
import java.awt.Color;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

/**
 *
 * @author zgw@dongying.pbc
 */
public class OutputPanel extends javax.swing.JPanel implements Output {

    private AttributeSet infoSet ;
    private AttributeSet errorSet;
    private AttributeSet promptSet;
    
    /** Creates new form OutputPanel */
    public OutputPanel() {
        initComponents();
        this.infoSet = this.getFontAttSet(Color.black, false, 12, null);
        this.errorSet = this.getFontAttSet(Color.red, true, 12, null);
        this.promptSet = this.getFontAttSet(Color.blue, true, 12, null);
    }

    /** This method is called from within the constructor to
     * initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        btnClear = new javax.swing.JButton();
        chkRoll = new javax.swing.JCheckBox();
        jScrollPane1 = new javax.swing.JScrollPane();
        txtOutput = new javax.swing.JTextPane();

        btnClear.setText("清空");
        btnClear.setName("btnClear"); // NOI18N
        btnClear.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnClearActionPerformed(evt);
            }
        });

        chkRoll.setSelected(true);
        chkRoll.setText("自动翻滚");
        chkRoll.setName("chkRoll"); // NOI18N

        jScrollPane1.setName("jScrollPane1"); // NOI18N

        txtOutput.setEditable(false);
        txtOutput.setName("txtOutput"); // NOI18N
        jScrollPane1.setViewportView(txtOutput);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap(373, Short.MAX_VALUE)
                .addComponent(chkRoll)
                .addGap(18, 18, 18)
                .addComponent(btnClear)
                .addContainerGap())
            .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 531, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 180, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(chkRoll)
                    .addComponent(btnClear)))
        );
    }// </editor-fold>//GEN-END:initComponents

    private void btnClearActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnClearActionPerformed
        this.txtOutput.setText("");
    }//GEN-LAST:event_btnClearActionPerformed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnClear;
    private javax.swing.JCheckBox chkRoll;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTextPane txtOutput;
    // End of variables declaration//GEN-END:variables

//    public void setTitle(String title){
//        this.txtTitle.setText(title);
//    }
    
    public void info(String msg) {
        this.insert(msg, infoSet);
        if (chkRoll.isSelected()) {
            txtOutput.setCaretPosition(txtOutput.getDocument().getLength());
        }
    }

    public void error(String msg, Exception e) {
        this.insert(msg, errorSet);
        if (chkRoll.isSelected()) {
            txtOutput.setCaretPosition(txtOutput.getDocument().getLength());
        }
    }
    
   public void error(String msg) {
        this.insert(msg, errorSet);
    }
    
    public void prompt(String msg) {
        this.insert(msg, promptSet);
        if (chkRoll.isSelected()) {
            txtOutput.setCaretPosition(txtOutput.getDocument().getLength());
        }
    }

    private void insert(String str, AttributeSet attrSet) {
        Document doc = txtOutput.getDocument();
        str = "\n " + str;
        try {
            doc.insertString(doc.getLength(), str, attrSet);
        } catch (BadLocationException e) {
            System.out.println("BadLocationException:   " + e);
        }
    }

    private void insert(String str, SimpleAttributeSet attrSet) {
        insert(str, attrSet);
    }
    
    private SimpleAttributeSet getFontAttSet(Color col, boolean bold, int size,String fontName){
        SimpleAttributeSet attrSet = new SimpleAttributeSet();
        //   颜色 
        StyleConstants.setForeground(attrSet, col);
        //   字体类型 
        if (bold == true) {
            StyleConstants.setBold(attrSet, true);
        }
        //   字体大小
        StyleConstants.setFontSize(attrSet, size);
        //   设置字体 
        if(fontName!=null){
            StyleConstants.setFontFamily(attrSet,fontName);
        }
        return attrSet;
    }




}
