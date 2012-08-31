/*
 * 
 * 
 */
package datatransfer.extract;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Column;
import org.executequery.databasemediators.DatabaseConnection;

import datatransfer.Output;
import datatransfer.util.DataTransferException;
import datatransfer.util.DdlUtil;
import pl.mpak.util.StringUtil;

/**
 *
 * @author zgw@dongying.pbc
 */
public class ModelImporter {

    private DatabaseConnection databaseConnection;
    private Database model;
    private String failedTable;
    private Output output;

    public ModelImporter(DatabaseConnection databaseConnection, Database model) {
        this.databaseConnection = databaseConnection;
        this.model = model;
    }

    /**
     * 创建临时表
     * @throws DataTransferException 
     */
    public void importModel() {
        output.prompt("在目标库中生成临时表结构（覆盖同名表并清空表中数据）");
        /*        Table[] tbs=model.getTables();
        for(int m=0;m<tbs.length;m++){
        Column[] cols=tbs[m].getColumns();
        for(int n=0;n<cols.length;n++){
        int oldsize=cols[n].getSizeAsInt();
        if(cols[n].getType().equalsIgnoreCase("varchar")||cols[n].getType().equalsIgnoreCase("decimal"))
        cols[n].setSize(Integer.toString(oldsize+5));
        System.out.println("cols[n].getType()"+cols[n].getType());
        System.out.println("cols[n].setSize()"+cols[n].getSizeAsInt());
        
        //cols[n].setSize(oldsize+5);
        }
        
        }*/
        failedTable = DdlUtil.createTables(databaseConnection, model);
        output.prompt("生成临时表完成");

        if (!StringUtil.isEmpty(this.getFailedTable())) {
            getOutput().error("下列表未创建成功:" + getFailedTable());
        }
    }

    /**
     * @return the printer
     */
    public Output getOutput() {
        return output;
    }

    /**
     * @param output the printer to set
     */
    public void setOutput(Output output) {
        this.output = output;
    }

    /**
     * @return the failedTable(创建失败的表)
     */
    public String getFailedTable() {
        return failedTable;
    }
}
