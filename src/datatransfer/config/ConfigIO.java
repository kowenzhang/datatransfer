/*
 * 
 * 
 */
package datatransfer.config;

import java.beans.IntrospectionException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import org.apache.commons.betwixt.io.BeanReader;
import org.apache.commons.betwixt.io.BeanWriter;
import org.apache.commons.betwixt.strategy.HyphenatedNameMapper;
import org.apache.ddlutils.io.LocalEntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author zgw@dongying.pbc
 */
public class ConfigIO {
    /**
     * Returns the commons-betwixt mapping file as an {@link org.xml.sax.InputSource} object.
     * Per default, this will be classpath resource under the path <code>/mapping.xml</code>.
     *  
     * @return The input source for the mapping
     */
    protected InputSource getBetwixtMapping()
    {
        return new InputSource(getClass().getResourceAsStream("/mapping_config.xml"));
    }
    
    /**
     * Returns a new bean reader configured to read DataTransferConfig models.
     * 
     * @return The reader
     */
    protected BeanReader getReader() throws IntrospectionException, SAXException, IOException
    {
        BeanReader reader = new BeanReader();

        reader.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(true);
        reader.getXMLIntrospector().getConfiguration().setWrapCollectionsInElement(false);
        reader.getXMLIntrospector().getConfiguration().setElementNameMapper(new HyphenatedNameMapper());
        reader.registerMultiMapping(getBetwixtMapping());
        
        return reader;
    }

    /**
     * Returns a new bean writer configured to writer DataTransferConfig models.
     * 
     * @param output The target output writer
     * @return The writer
     */
    protected BeanWriter getWriter(Writer output) throws ConfigException
    {
        try
        {
            BeanWriter writer = new BeanWriter(output);
    
            writer.getXMLIntrospector().register(getBetwixtMapping());
            writer.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(true);
            writer.getXMLIntrospector().getConfiguration().setWrapCollectionsInElement(false);
            writer.getXMLIntrospector().getConfiguration().setElementNameMapper(new HyphenatedNameMapper());
            writer.getBindingConfiguration().setMapIDs(false);
            writer.enablePrettyPrint();
    
            return writer;
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
    }

    /**
     * Reads the DataTransferConfig model contained in the specified file.
     * 
     * @param filename The model file name
     * @return The DataTransferConfig model
     */
    public DataTransferConfig read(String filename) throws ConfigException
    {
        DataTransferConfig model = null;

        try
        {
            model = (DataTransferConfig)getReader().parse(filename);
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
        return model;
    }

    /**
     * Reads the DataTransferConfig model contained in the specified file.
     * 
     * @param file The model file
     * @return The DataTransferConfig model
     */
    public DataTransferConfig read(File file) throws ConfigException
    {
        DataTransferConfig model = null;

        try
        {
            model = (DataTransferConfig)getReader().parse(file);
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
        return model;
    }

    /**
     * Reads the DataTransferConfig model given by the reader.
     * 
     * @param reader The reader that returns the model XML
     * @return The DataTransferConfig model
     */
    public DataTransferConfig read(Reader reader) throws ConfigException
    {
        DataTransferConfig model = null;

        try
        {
            model = (DataTransferConfig)getReader().parse(reader);
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
        return model;
    }

    /**
     * Reads the DataTransferConfig model from the given input source.
     *
     * @param source The input source
     * @return The DataTransferConfig model
     */
    public DataTransferConfig read(InputSource source) throws ConfigException
    {
        DataTransferConfig model = null;

        try
        {
            model = (DataTransferConfig)getReader().parse(source);
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
        return model;
    }

    /**
     * Writes the DataTransferConfig model to the specified file.
     * 
     * @param model    The DataTransferConfig model
     * @param filename The model file name
     */
    public void write(DataTransferConfig model, String filename) throws ConfigException
    {
        try
        {
            BufferedWriter writer = null;

            try
            {
                writer = new BufferedWriter(new FileWriter(filename));
    
                write(model, writer);
                writer.flush();
            }
            finally
            {
                if (writer != null)
                {
                    writer.close();
                }
            }
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
    }

    /**
     * Writes the DataTransferConfig model to the given output stream. Note that this method
     * does not flush the stream.
     * 
     * @param model  The DataTransferConfig model
     * @param output The output stream
     */
    public void write(DataTransferConfig model, OutputStream output) throws ConfigException
    {
        write(model, getWriter(new OutputStreamWriter(output)));
    }

    /**
     * Writes the DataTransferConfig model to the given output writer. Note that this method
     * does not flush the writer.
     * 
     * @param model  The DataTransferConfig model
     * @param output The output writer
     */
    public void write(DataTransferConfig model, Writer output) throws ConfigException
    {
        write(model, getWriter(output));
    }

    /**
     * Internal method that writes the DataTransferConfig model using the given bean writer.
     * 
     * @param model  The DataTransferConfig model
     * @param writer The bean writer
     */
    private void write(DataTransferConfig model, BeanWriter writer) throws ConfigException
    {
        try
        {
            writer.writeXmlDeclaration("<?xml version=\"1.0\"?>");
            writer.write(model);
        }
        catch (Exception ex)
        {
            throw new ConfigException(ex);
        }
    }
}
