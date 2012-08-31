package datatransfer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Properties;
import java.util.Date;
import java.text.DateFormat;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.*;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;

@SuppressWarnings("unchecked")
public class ImportDataIntoDb {

    private String BASE_PROPERTIES = null;
    private String BASE_PATH = null;
    private String DATA_PATH = null;
    private Properties props = null;
    private Connection con = null;
    private String drivername = null, url = null, username = null, password = null;
    private Database db = null;
    private Platform platform = null;
    private DataSource dataSource = null;

    public ImportDataIntoDb() {
    }

    @SuppressWarnings("unchecked")
    public void init() {
        BASE_PATH = System.getProperty("user.dir");
        BASE_PROPERTIES = BASE_PATH + "\\import.properties";
        props = new Properties();
        try {
            File f = new File(BASE_PROPERTIES);
            InputStream in = new FileInputStream(f);
            props.load(in);
            System.out.println("读取配置文件" + BASE_PROPERTIES + "成功！");
        } catch (IOException e) {
            System.out.println("不能读取配置文件" + BASE_PROPERTIES);
            e.printStackTrace();
        }
        try {
            System.out.println("开始连接数据库........................");
            DATA_PATH = props.getProperty("datapath").trim();
            drivername = props.getProperty("jdbc.driver").trim();
            url = props.getProperty("jdbc.url").trim();
            username = props.getProperty("jdbc.username").trim();
            password = props.getProperty("jdbc.password").trim();
            Class.forName(drivername);
            con = DriverManager.getConnection(url, username, password);
            Properties p = new Properties();
            p.put("driverClassName", drivername);
            p.put("username", username);
            p.put("password", password);
            p.put("url", url);
            p.put("poolPreparedStatements", "true");
            dataSource = BasicDataSourceFactory.createDataSource(p);
            platform = PlatformFactory.createNewPlatformInstance(dataSource);
            System.out.println("连接数据库成功！");
        } catch (Exception e) {
            System.out.println("连接数据库失败！");
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ImportDataIntoDb importer = null;
        Platform platform = null;
        Properties props = null;
        System.out.println("任务开始执行..............");
        importer = new ImportDataIntoDb();
        importer.init();
        try {
            platform = importer.getPlatform();
            props = importer.getProperties();
            String schemafilepath = importer.getDataPath() + "schema\\";
            File[] schemafiles = FileUtils.getFile(schemafilepath).listFiles();
            for (int m = 0; m < schemafiles.length; m++) {
                System.out.println("开始创建数据库" + schemafiles[m].getName() + "的表........................");
                // add time stamp column in schema file
                File tmp_fs = File.createTempFile("schema_tmp", ".xml");
                FileOutputStream tmp_fos = new FileOutputStream(tmp_fs);
                PrintWriter out = new PrintWriter(tmp_fos);
                BufferedReader schema_reader = null;
                try {
                    schema_reader = new BufferedReader(new FileReader(schemafiles[m]));
                    String single_schema = null;
                    while ((single_schema = schema_reader.readLine()) != null) {
                        out.println(single_schema);
                        if (single_schema.contains("<table name=")) {
                            out.println("\t  <column name=\"import_timestamp\" primaryKey=\"false\" required=\"false\" type=\"TIMESTAMP\" size=\"19\" default=\"CURRENT_TIMESTAMP\" autoIncrement=\"false\"/>");
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                out.flush();
                out.close();
                schema_reader.close();
                Database db = new DatabaseIO().read(tmp_fs);
                platform.createTables(db, true, true);
                // 计算那些表未成功创建
                Table[] tobecreatetbs = db.getTables();
                String url = importer.getUrl();
                Database newdb = platform.readModelFromDatabase(url.substring(url.lastIndexOf('/') + 1));
                Table[] existtbs = newdb.getTables();
                String un_success_tables_str = "";
                for (int a = 0; a < tobecreatetbs.length; a++) {
                    boolean success = false;
                    for (int p = 0; p < existtbs.length; p++) {
                        if (tobecreatetbs[a].getName().trim().equalsIgnoreCase(existtbs[p].getName().trim())) {
                            success = true;
                        }
                    }
                    if (!success) {
                        un_success_tables_str += tobecreatetbs[a].getName().trim() + ",";
                    }
                }
                System.out.print("创建数据库" + schemafiles[m].getName() + "的表完成");
                if (un_success_tables_str.length() > 1) {
                    System.out.println("，以下表未创建成功：" + un_success_tables_str);
                } else {
                    System.out.println("，全部表创建成功!");
                }

                File datadir = new File(importer.getDataPath() + "data\\");
                String[] insert_file_names = datadir.list();
                String delimiter = platform.getPlatformInfo().getSqlCommandDelimiter().trim();
                Date now = new Date();
                DateFormat df = DateFormat.getDateTimeInstance();
                String import_time = df.format(now);
                String[] createtbs = un_success_tables_str.split(",");
                for (int i = 0; i < insert_file_names.length; i++) {
                    boolean create = false;
                    for (int u = 0; u < createtbs.length; u++) {
                        if (createtbs[u].length() > 1) {
                            if (insert_file_names[i].contains(createtbs[u])) {
                                create = true;
                                break;
                            }
                        }
                    }
                    if (!create) {
                        File insertfile = new File(importer.getDataPath() + "data\\" + insert_file_names[i]);
                        System.out.println("开始导入文件: " + insertfile.getName() + "..........");
                        long allcount = 0;
                        long errcount = 0;
                        StringBuffer insert_sqls = new StringBuffer();
                        BufferedReader reader = null;
                        try {
                            reader = new BufferedReader(new FileReader(insertfile));
                            String single_sql = null;
                            while ((single_sql = reader.readLine()) != null) {
                                if (single_sql.length() > 1) {
                                    // add column import_timestamp
                                    single_sql = single_sql.substring(0, single_sql.indexOf("(") + 1) + "import_timestamp, "
                                            + single_sql.substring(single_sql.indexOf("(") + 1);
                                    single_sql = single_sql.substring(0, single_sql.lastIndexOf("VALUES (") + 8) + "\'" + import_time + "\', "
                                            + single_sql.substring(single_sql.lastIndexOf("VALUES (") + 8);
                                    insert_sqls.append(single_sql.trim() + delimiter + "\n");
                                    allcount++;
                                }
                            }
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            try {
                                if (reader != null) {
                                    reader.close();
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        String insert_sqls_str = insert_sqls.toString();
                        // 替换可能会导致insert出错的特殊字符。如：文本中存在"\'",db2在insert时将出错，需将"\'"替换为"''"
                        if (props.getProperty("code.replace") != null) {
                            String[] codes_needreplace = props.getProperty("code.replace").trim().split(";");
                            for (int p = 0; p < codes_needreplace.length; p++) {
                                String[] code_replace = codes_needreplace[p].split(",");
                                insert_sqls_str = insert_sqls_str.replace(code_replace[0], code_replace[1]);
                            }
                        }

                        if (props.getProperty("jdbc.schema") != null && props.getProperty("jdbc.schema").trim().length() > 0) {
                            errcount += platform.evaluateBatch(insert_sqls_str.replace("INSERT INTO ", "INSERT INTO "
                                    + props.getProperty("jdbc.schema").trim() + "."), true);
                        } else {
                            errcount += platform.evaluateBatch(insert_sqls_str, true);
                        }
                        reader.close();
                        reader = null;
                        insert_sqls = null;

                        if (errcount > 0) {
                            System.out.println("文件" + insertfile.getName() + "应导入数据为" + allcount + "，存在" + errcount + "条错误记录");
                        } else {
                            System.out.println("文件" + insertfile.getName() + "应导入数据为" + allcount + "条，全部导入成功！");
                        }
                        insertfile = null;
                    }
                }
                System.out.println("数据库" + schemafiles[m].getName() + "导入结束！");
                FileUtils.forceDelete(tmp_fs);
            }
            System.out.println("结束执行任务");
        } catch (Exception e) {
            System.out.println("发生严重错误！");
            e.printStackTrace();
        } finally {
            importer.destory();
        }
    }

    public void destory() {
        try {
            if (con != null) {
                con.close();
            }
        } catch (Exception e) {
        }
    }

    public String getDataPath() {
        return this.DATA_PATH;
    }

    public Database getDatabase() {
        return this.db;
    }

    public Properties getProperties() {
        return this.props;
    }

    public Platform getPlatform() {
        return this.platform;
    }

//    public Connection getConnection() {
//        return this.con;
//    }

    public String getUrl() {
        return this.url;
    }
}