package datatransfer.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.repository.DatabaseConnectionRepository;
import org.executequery.repository.RepositoryCache;

/**
 *
 * @author zgw@dongying.pbc
 */
public class DataTransferConfig {

    private String name = "未命名配置文件";
    private String description = "";
    private boolean changedAndUnsaved = false;
    private String configFile = "";
    private String destId;
    private Map<String, Map<String, UserExtractSql>> userExtractSqls = new HashMap<String, Map<String, UserExtractSql>>();
    private Map<String, TransferQuery> transferQuerys = new HashMap<String, TransferQuery>();
    
    public DataTransferConfig() {
        
    }
    
    public Map<String, TransferQuery> getAllTransferQuerys() {
        return transferQuerys;
    }

    public List<TransferQuery> getTransferQuerys() {
        ArrayList list = new ArrayList<TransferQuery>();
        list.addAll(transferQuerys.values());
        return list;
    }   
    

    public List<UserExtractSql> getUserExtractSqls() {
        ArrayList list = new ArrayList<UserExtractSql>();
        for(Map<String, UserExtractSql> map : userExtractSqls.values()){
            list.addAll(map.values());
        }
        return list;
    }

    public void setUserExtractSqls(Map<String, Map<String, UserExtractSql>> userExtractSqls) {
        this.userExtractSqls = userExtractSqls;
    }

    public String getUserExtractSql(String databaseName, String tableName) {
        Map<String, UserExtractSql> map = userExtractSqls.get(databaseName);
        if (null != map) {
            UserExtractSql ues = map.get(tableName);
            if(null!=ues){
                return ues.getSql();
            }else{
                return null;
            }
        } else {
            return null;
        }
    }

    public void putUserExtractSql(UserExtractSql userExtractSql) {        
        Map<String, UserExtractSql> map = userExtractSqls.get(userExtractSql.getDatabase());
        if (null == map) {
            map = new HashMap<String, UserExtractSql>();
            userExtractSqls.put(userExtractSql.getDatabase(), map);
        }
        map.put(userExtractSql.getTable(), userExtractSql);        
    }

    public String getDestId() {
        return destId;
    }

    public void setDestId(String destId) {
        this.destId = destId;
    }

    public Map<String, TransferQuery> getTransferQuerysMap() {
        return transferQuerys;
    }

    public void setTransferQuerys(Map<String, TransferQuery> transferQuerys) {
        this.transferQuerys = transferQuerys;
    }

    public void saveTransferQuerys(TransferQuery transferQuery) {
        transferQuerys.put(transferQuery.getDestinationTable(), transferQuery);
        setChangedAndUnsaved(true);
    }

    /**
     * @return the changedAndUnsaved
     */
    public boolean isChangedAndUnsaved() {
        return changedAndUnsaved;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
        setChangedAndUnsaved(true);
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
        setChangedAndUnsaved(true);
    }

    /**
     * @param changedAndUnsaved the changedAndUnsaved to set
     */
    public void setChangedAndUnsaved(boolean changedAndUnsaved) {
        this.changedAndUnsaved = changedAndUnsaved;
    }

    /**
     * @return the configFile
     */
    public String getConfigFile() {
        return configFile;
    }

    /**
     * @param configFile the currentConfigFile to set
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public DatabaseConnection getDatabaseConnectionById(String id) {


        return ((DatabaseConnectionRepository) RepositoryCache.load(DatabaseConnectionRepository.REPOSITORY_ID)).findById(id);
    }

    public DatabaseConnection getDatabaseConnectionByName(String name) {


        return ((DatabaseConnectionRepository) RepositoryCache.load(DatabaseConnectionRepository.REPOSITORY_ID)).findByName(name);
    }

    public DatabaseConnectionRepository getDatabaseConnectionRepository() {
        return ((DatabaseConnectionRepository) RepositoryCache.load(DatabaseConnectionRepository.REPOSITORY_ID));
    }

    public List<DatabaseConnection> getDatabaseConnections() {
        List<DatabaseConnection> list = ((DatabaseConnectionRepository) RepositoryCache.load(DatabaseConnectionRepository.REPOSITORY_ID)).findAll();
        return list;
    }

    /**
     * @return the destination
     */
    public DatabaseConnection getDestination() {
        DatabaseConnection destination = getDatabaseConnectionRepository().findById(destId);
        return destination;
    }

    /**
     * @param destination the destination to set
     */
    public void setDestination(DatabaseConnection databaseConnection) {
        this.destId = databaseConnection.getId();
        //    setChangedAndUnsaved(true);
    }
//    public void packSources() throws IOException {
//        List<TransferQuery> tss = this.getTransferQuerys();
//        Set<String> al = new HashSet<String>();
//        for (TransferQuery transferQuery : tss) {
//            //System.out.println(transferQuery.getEsql());
//            String sql = transferQuery.getSql();
//            StreamTokenizer stream = createTokenizer(new StringReader(sql));
//            while (stream.ttype != StreamTokenizer.TT_EOF) {
//                stream.nextToken();
//                if (stream.ttype == StreamTokenizer.TT_WORD && stream.sval.indexOf(".") > -1 && stream.sval.indexOf("[") > -1) {
//                    al.add(stream.sval);
//                }
//            }
//            Iterator it = al.iterator();
//            for (; it.hasNext();) {
//                String e = (String) it.next();
//                DataSource ds = this.getSourceByName(e.substring(e.indexOf("[") + 1, e.indexOf("]")));
//                Platform platform = PlatformFactory.createNewPlatformInstance(ds);
//                Database db = ds.getDatabase();
//                if (db == null) {
//                    if (ds.getSchema() != null) {
//                        if (ds.getSchema().trim().length() > 1) {
//                            ds.setSchema(ds.getSchema().toUpperCase());
//                        }
//                        db = platform.readModelFromDatabase(null, null, ds.getSchema(), null);
//                        ds.setDatabase(db);
//                    } else {
//                        db = platform.readModelFromDatabase(null);
//                        ds.setDatabase(db);
//                    }
//                }
//                String tableName = e.substring(e.indexOf("]") + 1, e.indexOf("."));
//                Table tb = db.getTableByName(tableName);
//                if (tb != null) {
//                    String colName = e.substring(e.indexOf(".") + 1);
//                    org.apache.ddlutils.model.Column cm = (Column) tb.getColumByName(colName);
//                    tb.addSelectedColumn(cm);
//                    db.addSelectedTable(tb);
//                }
//            }
//        }
//    }
//        private static StreamTokenizer createTokenizer(Reader r) {
//        StreamTokenizer stream = new StreamTokenizer(r);
//        stream.wordChars('.', '.');
//        stream.wordChars('_', '_');
//        stream.wordChars('[', '[');
//        stream.wordChars(']', ']');
//
//        if (!QueryBuilder.identifierQuoteString.equals("\"")) {
//            stream.quoteChar(QueryBuilder.identifierQuoteString.charAt(0));
//
////			for(int i=0; i<QueryBuilder.identifierQuoteString.length(); i++)
////			{
////				char wc = QueryBuilder.identifierQuoteString.charAt(i);
////				stream.wordChars(wc,wc);
////			}
//        }
//
//        stream.slashSlashComments(true);
//        stream.slashStarComments(true);
//
//        return stream;
//    }
}
