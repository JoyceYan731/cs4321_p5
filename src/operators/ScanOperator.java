package operators;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import data.DataBase;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.LogicalLogger;
import util.PhysicalLogger;
import util.TupleReader;

/**
 * this class provides function:
 * scan all tuples in a table 
 * 
 * @author Yixuan Jiang
 *
 */

public class ScanOperator extends Operator{
	
	private String tableName;
	private String tableAddress;
	private File tableFile;
	//private RandomAccessFile readPointer;
	private String tableAliase;
	private LinkedList<String> attributes;
	private TupleReader tr;
	

	/**
	 * This method is to get the next tuple after scanning
	 * 
	 * @return next tuple after scanning
	 */
	@Override
	public Tuple getNextTuple() {
		if (this.leftChild == null) {
			try {
				//String data = tr.readNextTuple().getTupleData();
				Tuple t = tr.readNextTuple();
				if (t != null) {
					/*Handle aliases*/
					//Tuple t = new Tuple(data, tableAliase, attributes);
					Expression e = this.getExpression();
					if(e != null) {
						while (t != null) {
							boolean res = super.judgeExpression(t);
							if(res) break;
							t = tr.readNextTuple();				
						}			
					}
					return t;
				}
			} catch (Exception e) {
				e.printStackTrace();
				e.getMessage();
			}
		}else {
			Tuple t = this.leftChild.getNextTuple();
			if (t != null) {
				Expression e = this.getExpression();
				if(e!=null) {
					while (t != null) {
						boolean res = super.judgeExpression(t);
						if(res) break;
						t = this.leftChild.getNextTuple();				
					}			
				}
				return t;
			}
		}
		
		
		return null;
	}

	/**
	 * This method is to reset scan operator
	 * by resetting its tuple reader
	 */
	@Override
	public void reset() {
		try {
			if(this.leftChild == null) {
				this.tr.reset();
			}else {
				this.leftChild.reset();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		
	}
	
	public void reset(int bufferIndex, int fileChannelIndex) {
		if (this.leftChild == null) {
			this.tr.resetBuffer(bufferIndex);
			this.tr.resetFileChannel(fileChannelIndex);
		}else {
			
		}
		
	}
	
	/**
	 * default constuctor
	 */
	public ScanOperator() {
		
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public ScanOperator(String tableInfo, Expression expression) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		this.tr = new TupleReader(tableInfo);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		setExpression(expression);
		
		//modification
		schema = tr.getSchema();
	}
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public ScanOperator(String tableInfo) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		this.tr = new TupleReader(tableInfo);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		//setExpression(expression);
		
		//modification
		schema = tr.getSchema();
		StringBuilder sb = new StringBuilder();
		sb.append("scan-");
		sb.append(tableAliase);
		name = sb.toString();
	}
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableName
	 * @param tableAliase
	 * 
	 */
	public ScanOperator(String tableName, String tableAliase, Expression expression) {
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(' ');
		sb.append(tableAliase);
		this.tr = new TupleReader(sb.toString());
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
		setExpression(expression);
		
		//modification
		schema = tr.getSchema();
		StringBuilder sb2 = new StringBuilder();
		sb2.append("scan-");
		sb2.append(tableAliase);
		name = sb2.toString();
		
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableName
	 * @param tableAliase
	 * @param operator
	 * 
	 */
	public ScanOperator(String tableName, String tableAliase, Expression expression, Operator op) {
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(' ');
		sb.append(tableAliase);
		this.tr = new TupleReader(sb.toString());
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
		setExpression(expression);
		setLeftChild(op);
		
		//modification
		schema = tr.getSchema();
		StringBuilder sb2 = new StringBuilder();
		sb2.append("scan-");
		sb2.append(tableAliase);
		name = sb2.toString();
	}
	
	/** get table aliase*/
	public String getTableAliase() {
		return tableAliase;
	}
	
	/** get table attributes*/
	public LinkedList<String> getAttributes(){
		return attributes;
	}
	
	/** get table name*/
	public String getTableName() {
		return tableName;
	}

	/** get table address*/
	public String getTableAddress() {
		return tableAddress;
	}

	/** get table file*/
	public File getTableFile() {
		return tableFile;
	}

	/** get table read pointer*/
	public TupleReader getReadPointer() {
		return tr;
	}

	@Override
	public void printPlan(int level) {
		StringBuilder path = new StringBuilder();
		String tableName = this.tableName;
		Expression exp = this.getExpression();
		
		if (exp!=null) {
			for (int i=0; i<level; i++) {
				path.append("-");
			}
			
			String[] array = exp.toString().split("AND");
			path.append("Select[");
			for (String str : array) {
				path.append(str.trim()).append(",");
			}
			
			path.deleteCharAt(path.length()-1);
			path.append("]");
			path.append(System.getProperty("line.separator"));
			
			level ++;
		}
		
		/* print scan operator*/
		for (int i=0; i<level; i++) {
			path.append("-");
		}
		path.append("TableScan[");
		path.append(tableName).append("]");
		
		PhysicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
		
	}
}