/*
 * 
 * 
 */
package datatransfer.config;

/**
 *
 * @author zgw@dongying.pbc
 */
public class ConfigException extends Exception{
    public ConfigException(String msg){
        super(msg);
    }
    
    public ConfigException(Throwable ex){
        super(ex);
    }
}
