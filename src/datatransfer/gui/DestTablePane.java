package datatransfer.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ItemEvent;
import java.util.List;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import nickyb.sqleonardo.querybuilder.QueryBuilder;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.executequery.GUIUtilities;
import org.executequery.databasemediators.DatabaseConnection;
import org.executequery.datasource.ConnectionManager;
import org.executequery.repository.RepositoryCache;

import datatransfer.config.TransferQuery;

public class DestTablePane extends JPanel {

    private JTable jTableSource;
    private JPanel jPanel1;
    private Database sourceDb;
    private Table[] sourceTables;
    private JComboBox jComboBoxSource;
    private JComboBox jComboBox1;
    private JScrollPane jScrollPane1;
    private QueryBuilder builder;
    private String[] destColumnNames;
    private String destId;

    /**
     * Create the panel.
     * @throws LdjsException 
     */
    public DestTablePane(QueryBuilder builder) {

        this.builder = builder;
        JLabel lblSource = new JLabel("目标库");
        jComboBoxSource= new JComboBox();
        jComboBoxSource.setPreferredSize(new Dimension(80, 20));
        jComboBoxSource.setMinimumSize(new Dimension(10, 20));
        jComboBoxSource.addItemListener(new java.awt.event.ItemListener() {

            public void itemStateChanged(java.awt.event.ItemEvent evt) {
            	jComboBoxSourceItemStateChanged(evt);
            }
        });
        JLabel lblNewLabel = new JLabel("目标表");
        jComboBox1 = new JComboBox();
        jComboBox1.setPreferredSize(new Dimension(80, 20));
        jComboBox1.setMinimumSize(new Dimension(10, 20));
        jComboBox1.addItemListener(new java.awt.event.ItemListener() {

            public void itemStateChanged(java.awt.event.ItemEvent evt) {
                jComboBox1ItemStateChanged(evt);
            }
        });
        jPanel1 = new JPanel();
        jPanel1.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
        jPanel1.add(lblSource);
        jPanel1.add(jComboBoxSource);
        
        jPanel1.add(lblNewLabel);
        jPanel1.add(jComboBox1);
        jTableSource = new JTable();
        jTableSource.setMinimumSize(new Dimension(50, 60));
        jScrollPane1 = new JScrollPane(jTableSource);

        setLayout(new BorderLayout(0, 0));
        add(jPanel1, BorderLayout.PAGE_START);
        add(jScrollPane1, BorderLayout.CENTER);
        jComboBox1.setRenderer(new DefaultListCellRenderer() {

            @Override
            public Component getListCellRendererComponent(JList list, Object value, int index,
                    boolean isSelected, boolean cellHasFocus) {
                super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                if(null!=value)
                setText(((Table) value).getName());

                return this;
            }
        });
        
      loadData();
    }

    public void loadData() {
    	//DatabaseConnection
    	setDestDataSource();
    	DatabaseConnection databaseConnection=(DatabaseConnection)jComboBoxSource.getSelectedItem();
       //System.out.println(databaseConnection.getName());
    	sourceDb =ConnectionManager.getDataBase(databaseConnection); //;
        sourceTables = sourceDb.getTables();
        jComboBox1.setModel(new DefaultComboBoxModel(sourceTables));
        if(null!=jComboBox1.getSelectedItem())
        jTableSource.setModel(loadTable((Table) jComboBox1.getSelectedItem()));
       // loadTransferQuery();
    }
    private void setDestDataSource()
    {
    	List<DatabaseConnection> connections=builder.getDatabaseConnections();
    	jComboBoxSource.setModel(new DefaultComboBoxModel(connections.toArray()));
    	String destId=RepositoryCache.getCurrentConfig().getDestId();
    	if(null!=destId)
    		{DatabaseConnection dc=RepositoryCache.getCurrentConfig().getDestination();
    		 if(null==dc)
    			 dc=connections.get(0);
    		jComboBoxSource.setSelectedItem(dc);
    		this.setDestId(destId); 
    		}
    	else
    		this.setDestId(((DatabaseConnection)jComboBoxSource.getSelectedItem()).getId()); 
    }
    private void jComboBox1ItemStateChanged(java.awt.event.ItemEvent evt) {
        // TODO add your handling code here:
        if (evt.getStateChange() == ItemEvent.SELECTED) {
        	jTableSource.setModel(loadTable((Table) jComboBox1.getSelectedItem()));
        	loadTransferQuery();
            //   this.builder.getTransferQuery().setDestinationTable(((Table)jComboBox1.getSelectedItem()).toString());
        }
    }
    private void loadTransferQuery()
    {
    	
    if (RepositoryCache.getCurrentConfig().getTransferQuerysMap().get(((Table) jComboBox1.getSelectedItem()).getName()) != null) {
        this.builder.loadQuery(RepositoryCache.getCurrentConfig().getTransferQuerysMap().get(((Table) jComboBox1.getSelectedItem()).getName()));
    } else {
        TransferQuery tq = new TransferQuery();
        tq.setDestinationTable(((Table) jComboBox1.getSelectedItem()).getName());
        this.builder.loadQuery(tq);
    }
    	
    }
    
    private void jComboBoxSourceItemStateChanged(ItemEvent evt)
    {
    	try{
    	if (evt.getStateChange() == ItemEvent.SELECTED) {
     	this.setDestId(((DatabaseConnection)jComboBoxSource.getSelectedItem()).getId());
    	DatabaseConnection dc=(DatabaseConnection)jComboBoxSource.getSelectedItem();
        sourceDb = ConnectionManager.getDataBase(dc);
        sourceTables = sourceDb.getTables();
        jComboBox1.setModel(new DefaultComboBoxModel(sourceTables));
        if(null!=jComboBox1.getSelectedItem()) 
        jTableSource.setModel(loadTable((Table) jComboBox1.getSelectedItem()));
        }
    	}
    	catch (Exception e) {
    		 GUIUtilities.displayExceptionErrorDialog("无法建立到数据库"+((DatabaseConnection) jComboBoxSource.getSelectedItem()).getName()+"的连接", e);
		}
    	
    }
    private DefaultTableModel loadTable(Table tab) {
        Vector<String> tableHeaders = new Vector<String>();
        tableHeaders.add("序号");
        tableHeaders.add("列名");
        tableHeaders.add("注释");
        tableHeaders.add("类型");

        Vector tableData = new Vector();
        destColumnNames = new String[tab.getColumns().length];
        //oneRow.add(tab.getId());
        for (int i = 0; i < tab.getColumns().length; i++) {
            Vector oneRow = new Vector();
            oneRow.add(i + 1);
            oneRow.add(tab.getColumns()[i].getName());
            destColumnNames[i] = tab.getColumns()[i].getName();
            oneRow.add(tab.getColumns()[i].getDescription());
            oneRow.add(tab.getColumns()[i].getType());
            tableData.add(oneRow);
        }

        return new DefaultTableModel(tableData, tableHeaders);
    }

    public String[] getDestColumnNames() {
        return destColumnNames;
    }

    public void setDestColumnNames(String[] destColumnNames) {
        this.destColumnNames = destColumnNames;
    }

    public String getDestId() {
		return destId;
	}

	public void setDestId(String destId) {
		this.destId = destId;
	}

	public String getTableName() {
        return ((Table) jComboBox1.getSelectedItem()).getName();
    }

    public String SetTable(String tableName) {

        return ((Table) jComboBox1.getSelectedItem()).getName();
    }

    public void setDestTable(String destinationTable) {
    	setDestDataSource();
        jComboBox1.setSelectedIndex(-1);
        for (int i = 0; i < sourceTables.length; i++) {
            Table t = sourceTables[i];
            if (t.getName().equals(destinationTable)) {
                if (jComboBox1.getSelectedIndex() != i) {
                    jComboBox1.setSelectedIndex(i);
                } else {
                    jComboBox1.setSelectedIndex(-1);
                    jComboBox1.setSelectedIndex(i);
                }
            }
        }

    }
}
