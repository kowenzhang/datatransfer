/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datatransfer;

/**
 *
 * @author kowen
 */
public interface Output {
    /**
     * 输出一行信息
     * @param msg 
     */
    public void info(String msg);
    
    /**
     * 输出错误
     * @param msg
     * @param e 
     */
    public void error(String msg,Exception e);
    
    /**
     * 输出错误
     * @param msg
     */
    public void error(String msg);
    
    /**
     * 输出重要提示
     * @param msg 
     */
    public void prompt(String msg);
    
}
