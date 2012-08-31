/*
 * 
 * 
 */
package datatransfer.gui.swing;

import javax.swing.*;
import java.awt.*;
import javax.swing.text.*;
import java.io.*;

public class Test {

    JFrame frame = null;
    JTextPane textPane = null;
//    File file = null;
//    Icon image = null;

    public Test() {
        frame = new JFrame("JTextPane ");
        textPane = new JTextPane();
//        file = new File("./classes/test/icon.gif ");
//        image = new ImageIcon(file.getAbsoluteFile().toString());
    }

    public void insert(String str, AttributeSet attrSet) {
        Document doc = textPane.getDocument();
        str = "\n " + str;
        try {
            doc.insertString(doc.getLength(), str, attrSet);
        } catch (BadLocationException e) {
            System.out.println("BadLocationException:   " + e);
        }
    }

    public void setDocs(String str, Color col, boolean bold, int fontSize) {
        SimpleAttributeSet attrSet = new SimpleAttributeSet();
        StyleConstants.setForeground(attrSet, col);
//   颜色 
        if (bold == true) {
            StyleConstants.setBold(attrSet, true);
        }//   字体类型 
        StyleConstants.setFontSize(attrSet, fontSize);
//   字体大小 
//   StyleConstants.setFontFamily(attrSet,   "黑体 "); 
//   设置字体 
        insert(str, attrSet);
    }

    public void gui() {
//        textPane.insertIcon(image);
        setDocs("第一行的文字 ", Color.red, false, 20);
        setDocs("第二行的文字 ", Color.BLACK, true, 25);
        setDocs("第三行的文字 ", Color.BLUE, false, 20);
        frame.getContentPane().add(textPane, BorderLayout.CENTER);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(200, 300);
        frame.setVisible(true);
    }

    public static void main(String[] args) {
        Test test = new Test();
        test.gui();
    }
}
