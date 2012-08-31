/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datatransfer.db;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 *
 * @author zgw
 */
public class DriverClassLoader extends URLClassLoader {

  public DriverClassLoader(URL url) {
    super(new URL[] {url}, Thread.currentThread().getContextClassLoader());
  }

  public DriverClassLoader(String url) throws MalformedURLException {
    super(new URL[] {new File(url).toURI().toURL()}, Thread.currentThread().getContextClassLoader());
  }

  public java.sql.Driver getDriver(String driverClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return (java.sql.Driver)loadClass(driverClass).newInstance();
  }

  @Override
  public void addURL(URL url) {
    super.addURL(url);
  }

}
