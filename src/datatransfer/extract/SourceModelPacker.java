/*
 * 
 * 
 */
package datatransfer.extract;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import nickyb.sqleonardo.querybuilder.QueryBuilder;
import nickyb.sqleonardo.querybuilder.QueryModel;
import nickyb.sqleonardo.querybuilder.syntax.QueryTokens;
import nickyb.sqleonardo.querybuilder.syntax.QueryTokens._Expression;
import nickyb.sqleonardo.querybuilder.syntax.SQLFormatter;
import nickyb.sqleonardo.querybuilder.syntax.SQLParser;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.executequery.datasource.ConnectionManager;

import datatransfer.Output;
import datatransfer.config.DataTransferConfig;
import datatransfer.config.TransferQuery;
import datatransfer.util.DataTransferException;

/**
 * 
 * @author zgw@dongying.pbc
 */
public class SourceModelPacker {

	private DataTransferConfig config;
	/**
	 * Map<sourceName,Map<tableName,set<columnName>>>
	 */
	private Map<String, Map<String, Set<String>>> need;
	//      map<databasename,map<tablename,wherecondition>> conditions;每一个表一个查询条件
	private Map<String, Map<String, String>> conditions;
	private Map<String, Database> models = new HashMap<String, Database>();
	private Database packedModel = new Database();
	private Output output;

	public SourceModelPacker(DataTransferConfig config) {
		this.config = config;
	}

	public Map<String, Map<String, String>> getConditions() {
		return conditions;
	}

	public void setConditions(Map<String, Map<String, String>> conditions) {
		this.conditions = conditions;
	}

	/**
	 * 封装所有原始库数据结构，并包装所需要的表和列到packedModel
	 */
	public void pack() throws DataTransferException {
		this.packNeed();
		printNeeded();
		this.parseCondition();
		this.packModels();
		output.info("封装为目标库临时表结构");
		for (Database db : models.values()) {
			packedModel.mergeWith(db);
		}
		ConnectionManager.setDestdb(packedModel);
	}

	/**
	 * 分析所需数据库结构
	 * 
	 * @throws DataTransferException
	 */
	private void packNeed() throws DataTransferException {
		List<TransferQuery> tss = config.getTransferQuerys();
		Set<String> al = new HashSet<String>();
		need = new HashMap<String, Map<String, Set<String>>>();
		for (TransferQuery transferQuery : tss) {
			String sql = transferQuery.getSql();
			StreamTokenizer stream = createTokenizer(new StringReader(sql));
			while (stream.ttype != StreamTokenizer.TT_EOF) {
				try {
					stream.nextToken();
				} catch (IOException ex) {
					throw new DataTransferException("分析所需表结构错误:" + sql, ex);
				}
				if (stream.ttype == StreamTokenizer.TT_WORD
						&& stream.sval.indexOf(".") > -1
						&& stream.sval.indexOf("[") > -1) {
					al.add(stream.sval);
				}
			}
			Iterator it = al.iterator();
			while (it.hasNext()) {
				String e = (String) it.next();
				boolean empty = false;
				String dsName = e.substring(e.indexOf("[") + 1, e.indexOf("]"));
				if (!need.keySet().contains(dsName)) {
					need.put(dsName, new HashMap<String, Set<String>>());
					empty = true;
				}
				Map<String, Set<String>> tables = need.get(dsName);
				String tableName = e.substring(e.indexOf("]") + 1,
						e.indexOf("."));
				if (empty || !tables.keySet().contains(tableName)) {
					tables.put(tableName, new HashSet<String>());
					empty = true;
				} else {
					empty = false;
				}
				String colName = e.substring(e.indexOf(".") + 1);
				Set<String> cols = tables.get(tableName);
				if (empty || !cols.contains(colName)) {
					cols.add(colName);
				}
			}
		}
       System.out.println("需要创建的表和列:"+need.toString());
	}

	private void parseCondition() throws DataTransferException {
		List<TransferQuery> tss = config.getTransferQuerys();
		for (TransferQuery transferQuery : tss) {
			getConditions(transferQuery.getSql());
		}
		if (null != conditions) {
			System.out.println("begin write whereconditon");
			for (Object o : conditions.keySet())

				System.out.println(o.toString() + ":"
						+ conditions.get(o).toString());
		}
	}

	private void getConditions(String sql) {
		if (null == conditions)
			conditions = new HashMap<String, Map<String, String>>();
		try {
			QueryModel qm = SQLParser.toQueryModel(sql);
			/*
			 * //parse fromcluse begin;
			 * //分析连接语句，加入同数据源的innerjoin，同时检查，该表是否存在于left join的right表中，如果有则加入。
			 * QueryTokens._TableReference[] ref =
			 * qm.getQueryExpression().getQuerySpecification().getFromClause();
			 * QueryTokens._TableReference tblRef=ref[ref.length-1];
			 * if(ref.length>0 && tblRef instanceof QueryTokens.Join) { int
			 * joinType = ((QueryTokens.Join)tblRef).getType();
			 * 
			 * //inner join, DatabaseConnection
			 * dc1=((QueryTokens.Join)tblRef).getPrimary
			 * ().getTable().getDatabaseConnection(); DatabaseConnection
			 * dc2=((QueryTokens
			 * .Join)tblRef).getForeign().getTable().getDatabaseConnection();
			 * if(dc1.equals(dc2)) { if(joinType==0) { String
			 * fromclu=tblRef.toString();}
			 * if(joinType==2&&((QueryTokens.Join)tblRef
			 * ).getForeign().getTable().getName().equals(tableName)) { String
			 * fromclu=tblRef.toString();} } }
			 */
			// parse fromcluse begin;//分析where condition从中挖掘该表的数据条件：
			// 1、根据or的数量将whereclause分为几个子段。
			// 2.1在每一个子段中对于同一个表必须都要出现普通常量条件，如果出现则转到3.1，否则返回。
			// 3.1 查找该表的所有普通常量条件。

			QueryTokens.Condition[] whereCondition = qm.getQueryExpression()
					.getQuerySpecification().getWhereClause();
			List<ArrayList> list = new ArrayList<ArrayList>();
			List list1 = new ArrayList();
			// 根据 or的数量把whereclause分为几个子段
			for (QueryTokens.Condition qc : whereCondition) {
				list1.add(qc);
				String append = qc.getAppend();
				if (null != append && append.equalsIgnoreCase("OR")) {
					list.add((ArrayList) copyTo(list1, qc.getClass()));
					list1.clear();
				}
			}
			if (!list1.isEmpty())
				list.add((ArrayList) copyTo(list1,
						new QueryTokens.Condition().getClass()));
			for (Object dcName : need.keySet()) {
			    
				System.out.println("KEY SET IS:"+need.keySet().toString());
				System.out.println("key  "+ dcName.toString()+"  excuted@@");
				Map map = need.get(dcName);
				for (Object tableName : map.keySet()) {
					// 对每一个表，遍历每一个子段
					// 查看每个子段中同一dc.table是否同时出现，如果同时出现，则该表的查询条件可以保留
					// 具体执行方法：遍历每一个子段，如果该子段有该表的常量条件则存储，如果没有则要把以前存储的该表的条件全部清除，并且返回。
					if (!conditions.containsKey(dcName))
						conditions.put((String) dcName,
								new HashMap<String, String>());
					Map mapTableCon = conditions.get(dcName);
					if (!mapTableCon.containsKey((String) tableName)) 
					{
						mapTableCon.put(tableName, "");
					}
					for (int i = 0; i < list.size(); i++) {
						List l = list.get(i);
						long start = Calendar.getInstance().getTimeInMillis();
						String temp = getCondtionForTab((String) dcName,
								(String) tableName, l);
						long end = Calendar.getInstance().getTimeInMillis();
						System.out.println("parse conditions cost:"
								+ (end - start));
						if (temp.length() == 0) {
							mapTableCon.remove(tableName);
							break;
						} else
						{
							String wherecon = (String) mapTableCon
									.get(tableName);
							mapTableCon.put(tableName, wherecon + temp);
						}
					}
				} 
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String getCondtionForTab(String dcName, String tableName, List list) {

		String whereCon = "";
		// 每个子段的执行3.1
		for (QueryTokens.Condition qc : (List<QueryTokens.Condition>) list) {
			_Expression left = qc.getLeft();
			_Expression right = qc.getRight();
			// 两端是都是表的字段
			boolean isColumnLeft = isColumn(left);
			boolean isColumnRight = isColumn(right);
			if (isColumnLeft && isColumnRight) {

				String leftS = left.toString();
				int dot = leftS.lastIndexOf(SQLFormatter.DOT);
				String tbLeft = leftS.substring(leftS.indexOf("]") + 1, dot);
				String rightS = right.toString();
				dot = rightS.lastIndexOf(SQLFormatter.DOT);
				String tbRight = rightS.substring(rightS.indexOf("]") + 1, dot);
				// 表同属于该databaseconnection
				if (leftS.substring(leftS.indexOf("[") + 1,
						leftS.indexOf("]") + 1).equals(
						rightS.substring(rightS.indexOf("[") + 1,
								rightS.indexOf("]") + 1))
						&& leftS.substring(leftS.indexOf("[") + 1,
								leftS.indexOf("]") + 1)
								.equalsIgnoreCase(dcName)) {
					// 字段同属于该表
					if (tbLeft.equals(tbRight)
							&& tbLeft.equalsIgnoreCase(tableName)) {
						// 该条件保留,添加至whereclause
						whereCon = whereCon + " "
								+ qc.toString().replaceAll("\\[[^.]*\\.", "");
					}
				}
			}
			if (!isColumnLeft && !isColumnRight) {
				// mapTableCon.remove(tableName);
			} else {
				// 如果一个表达式的一侧是一个column，另一侧不是column（若是包含column的表达式怎么办？），并且这个表达式是该数据源中该表的column则保留
				String tbs = null;

				if (isColumnLeft) {
					tbs = left.toString();
				}
				if (isColumnRight) {
					tbs = right.toString();
				}

				int dot = tbs.lastIndexOf(SQLFormatter.DOT);

				String tb = tbs.substring(tbs.indexOf("]") + 1, dot);
				String dc = tbs.substring(tbs.indexOf("[") + 1,
						tbs.indexOf("]"));
				if (tb != null && dc.equals(dcName) && tb.equals(tableName)) {
					// 该条件保留,添加至whereclause
					whereCon = whereCon + " "
							+ qc.toString().replaceAll("\\[[^.]*\\.", "");
				}
			}
		}
		return whereCon;
	}

	public Boolean isColumn(QueryTokens._Expression e)

	{
		boolean flag = false;
		try {

			StreamTokenizer stream = createTokenizer(new StringReader(
					e.toString()));
			for (ArrayList al = new ArrayList(); true;) {
				stream.nextToken();
				if (stream.ttype == StreamTokenizer.TT_EOF) {
					ListIterator li = al.listIterator();

					String ref = li.next().toString();
					String alias = null;

					while (li.hasNext()) {
						String next = li.next().toString();

						if (next.toString().equals(
								String.valueOf(SQLFormatter.DOT))
								|| ref.endsWith(String
										.valueOf(SQLFormatter.DOT)))
							ref = ref + next;
						else
							alias = next;
					}

					ref = SQLFormatter.stripQuote(ref);
					int dot = ref.lastIndexOf(SQLFormatter.DOT);
					if (dot != -1) {
						String owner = ref.substring(ref.indexOf("]") + 1, dot);
						String cname = ref.substring(dot + 1);
						System.out.println(ref);
						flag = true;
						return flag;
					} else {
						flag = false;
						return flag;
					}

				} else {
					if (stream.sval == null
							&& (char) stream.ttype != SQLFormatter.DOT)
						break;
					al.add(stream.sval == null ? String
							.valueOf(SQLFormatter.DOT) : stream.sval);
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return flag;
	}

	public <E> List<E> copyTo(List<?> source, Class<E> destinationClass)
			throws IllegalAccessException, InvocationTargetException,
			InstantiationException {
		if (source.size() == 0)
			return Collections.emptyList();
		List<E> res = new ArrayList<E>(source.size());
		for (Object o : source) {
			E e = destinationClass.newInstance();
			BeanUtils.copyProperties(e, o);
			res.add(e);
		}
		return res;
	}

	/**
	 * 获取所需数据源的Database，并根据所需数据结构need，去除不需要的表和列
	 */
	private void packModels() {
		output.info("从原始库提取需要的表和列");
		for (String dsName : need.keySet()) {
			Database db = ConnectionManager.getDataBase(config
					.getDatabaseConnectionByName(dsName));
			for (Table table : db.getTables()) {
				if (need.get(dsName).containsKey(table.getName())) {
					for (org.apache.ddlutils.model.Column column : table
							.getColumns()) {
						if (!need.get(dsName).get(table.getName())
								.contains(column.getName())) {
							table.removeColumn(column);
						}
					}
				} else {
					db.removeTable(table);
				}
			}
			models.put(dsName, db);
		}
	}

	public void printNeeded() {
		output.prompt("分析源数据源库所需要的表和列：");
		for (String dsName : need.keySet()) {
			Map<String, Set<String>> tableNeed = need.get(dsName);
			StringBuffer bf = new StringBuffer();
			bf.append(dsName + "{");
			for (String tableName : tableNeed.keySet()) {
				bf.append(tableName + "[");
				for (String colName : tableNeed.get(tableName)) {
					bf.append(colName + ",");
				}
				bf.substring(0, bf.length() - 1);
				bf.append("]");
			}
			bf.append("}");
			output.info(bf.toString());
		}
	}

	/**
	 * 获取封装好的数据库模型
	 * 
	 * @return Database
	 */
	public Database getPackedModel() {
		return packedModel;
	}

	/**
	 * 获取封装过的原始库数据模型
	 * 
	 * @param sourceName
	 * @return
	 */
	public Database getPackedModelBySource(String sourceName) {
		return models.get(sourceName);
	}

	/**
	 * @return the printer
	 */
	public Output getOutput() {
		return output;
	}

	/**
	 * @param printer
	 *            the printer to set
	 */
	public void setOutput(Output printer) {
		this.output = printer;
	}

	private static StreamTokenizer createTokenizer(Reader r) {
		StreamTokenizer stream = new StreamTokenizer(r);
		stream.wordChars('.', '.');
		stream.wordChars('_', '_');
		stream.wordChars('[', '[');
		stream.wordChars(']', ']');

		if (!QueryBuilder.identifierQuoteString.equals("\"")) {
			stream.quoteChar(QueryBuilder.identifierQuoteString.charAt(0));

			// for(int i=0; i<QueryBuilder.identifierQuoteString.length(); i++)
			// {
			// char wc = QueryBuilder.identifierQuoteString.charAt(i);
			// stream.wordChars(wc,wc);
			// }
		}

		stream.slashSlashComments(true);
		stream.slashStarComments(true);

		return stream;
	}
}
