/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datatransfer.dbtype;

import java.beans.IntrospectionException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.apache.commons.betwixt.io.BeanReader;
import org.apache.commons.betwixt.io.BeanWriter;
import org.apache.commons.io.FileUtils;
import org.apache.ddlutils.io.DatabaseIO;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author kowen
 */
public class DatabaseTypeManager {

    public DatabaseTypes getDatabaseTypes() throws IntrospectionException, IOException, SAXException {
        File file = new File("sd");

        BeanReader beanReader = new BeanReader();
        //配置BeanReader实例 

        beanReader.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(false);
        beanReader.getBindingConfiguration().setMapIDs(false); //不自动生成ID 
        //注册要转换对象的类，并指定根节点名称 
        beanReader.registerBeanClass("databaseTypes", DatabaseTypes.class);

        //将XML解析Java Object 
        DatabaseTypes databaseTypes = (DatabaseTypes) beanReader.parse(file);
        return databaseTypes;
    }

    public void saveDatabaseTypes() throws FileNotFoundException, IntrospectionException, IOException, SAXException {
        StringWriter outputWriter = new StringWriter();
        BeanWriter writer = new BeanWriter(outputWriter);

        writer.getXMLIntrospector().register(new InputSource(getClass().getResourceAsStream("/mapping.xml")));
        writer.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(true);
        writer.getXMLIntrospector().getConfiguration().setWrapCollectionsInElement(false);
        writer.getBindingConfiguration().setMapIDs(false);
        writer.enablePrettyPrint();
//        beanWriter.write(person,ins); 
//        FileUtils.writeStringToFile(result, dlxml, "UTF-8", false);
    }
}
