/*
 * 
 * 
 */
package datatransfer.util;

/**
 *
 * @author zgw@dongying.pbc
 */
public class DataTransferException extends Exception {

    public DataTransferException(String msg) {
        super(msg);
    }
    
    public DataTransferException(String msg,Throwable ex) {
        super(msg+":"+ex.getMessage(),ex);
    }

    public DataTransferException(Throwable ex) {
        super(ex);
    }
}
