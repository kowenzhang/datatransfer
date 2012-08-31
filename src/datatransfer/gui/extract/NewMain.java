/*
 * 
 * 
 */
package datatransfer.gui.extract;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author zgw@dongying.pbc
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException {
        MyThread t = new MyThread();
        t.join();
    }
    
    
}
class MyThread  extends Thread{
        @Override
        public void run(){
            while(true){
                try {
                    sleep(500);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                int i=0;            
                System.out.println("running"+i++);   
            }
        }
    }
