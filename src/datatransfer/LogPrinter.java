/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datatransfer;

/**
 *
 * @author kowen
 */
public interface LogPrinter {
    
    public void log(String msg);
    
    public void error(String msg,Exception e);
    
}
