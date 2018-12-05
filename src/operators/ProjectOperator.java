package operators;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import data.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.LogicalLogger;
import util.PhysicalLogger;

/**
 * This class provides function:
 * Choose required attributes according 
 * to the SELECT requirement
 * 
 * @author Xiaoxing Yan
 */
public class ProjectOperator extends Operator{
	
	/*store information of needed attributes*/
	private List<SelectItem> selectItems;
	/*check that whether return all attributes or return specific attributes*/
	private boolean allColumns = false;
	/*memorize the primitive order of relations in SQL queries. String: tableAlias; Integer: the index*/
	private Map<String, Integer> aliasOrder;
	
	/** 
	 * This method is a constructor which is to
	 * get corresponding columns information and initialize childOp.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */

	public ProjectOperator(PlainSelect plainSelect,Operator op) {
		super.setLeftChild(op);
		StringBuilder sb = new StringBuilder();
		sb.append("proj-");
		sb.append(op.name);
		name = sb.toString();
		selectItems = plainSelect.getSelectItems();
		if (selectItems.get(0).toString() == "*") {
			allColumns = true;
			this.schema = op.schema;
		}else {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (int i = 0; i < selectItems.size(); i++) {
				map.put(selectItems.get(i).toString(), i);
			}
			this.schema = map;
		}
		
		
	}
	
	public ProjectOperator(List<SelectItem> sI, Operator op, List<String> aliasOrder) {
		super.setLeftChild(op);
		StringBuilder sb = new StringBuilder();
		sb.append("proj-");
		sb.append(op.name);
		name = sb.toString();
		this.selectItems = sI;
		this.aliasOrder = new HashMap<String, Integer>();
		if (selectItems.get(0).toString() == "*") {
			allColumns = true;
			this.schema = op.schema; // in projectOperator, the schema of the operator is consistent with its join operator.
			// but the schema of its output tuples are the same with the ps statement.
		}else {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (int i = 0; i < selectItems.size(); i++) {
				map.put(selectItems.get(i).toString(), i);
			}
			this.schema = map;
		} 
		for (int i = 0; i < aliasOrder.size(); i++) {
		    this.aliasOrder.put(aliasOrder.get(i), i);
		}
	}

	
	/**
	 * This method is to get the next tuple after projection
	 * 
	 * @return next tuple after projection
	 */
	@Override
	public Tuple getNextTuple() {
		Operator child = getLeftChild();
		Tuple current = child.getNextTuple();
		if (current != null && !allColumns) {
			/*Assume there must be corresponding columns in the given tuple*/
			long[] data = new long[selectItems.size()];
			Map<String, Integer> schema = new HashMap<String, Integer>();
			int index = 0;
			boolean flag = false;
			for (SelectItem expre : selectItems) {
				String attributeName = expre.toString();
				Integer dataIndex = current.getSchema().get(attributeName);
				if (dataIndex!=null) {
					flag = true;
					data[index] = current.getData()[dataIndex];
					schema.put(attributeName, index);
					index++;
				}	
			}
			if (flag) {
				current = new Tuple(data, schema);
			}else {
				current = null;
			}
			
		}
		reAlignColumn(current);
		return current;
	}

	/** p4 update: renew the column order such that the column listed as the table order
	 * mentioned in SQL queries;
	 */
	
	private void reAlignColumn(Tuple currTp) {
		if (currTp == null) {
			return;
		}
		Map<String, Integer> currSchema = currTp.getSchema();
		List<String> attributes = new ArrayList<>(this.schema.keySet());
		Collections.sort(attributes, new ColumnComparator());
		long[] newData = new long[currSchema.size()];
		for (int i = 0; i < attributes.size(); i++) {
			newData[i] = currTp.getData()[currSchema.get(attributes.get(i))];
			currSchema.put(attributes.get(i), i);
		}
		
		currTp.setData(newData);
	}
	
	/**
	 * This method is to reset project operator
	 * by resetting its child operator
	 */
	@Override
	public void reset() {
		getLeftChild().reset();
		
	}
	
	private class ColumnComparator implements Comparator<String> {
		@Override 
		public int compare(String s1, String s2) {
			String[] s1List = s1.split("\\.");
			String[] s2List = s2.split("\\.");
			if (aliasOrder.get(s1List[0]) != aliasOrder.get(s2List[0])) {
				return aliasOrder.get(s1List[0]) - aliasOrder.get(s2List[0]);
			} else {
				return schema.get(s1) - schema.get(s2);
			}
		}
	}
	
	
	public List<SelectItem> getSelectItems() {
		return this.selectItems;
	}
	
	@Override
	public void printPlan(int level) {
		StringBuilder path = new StringBuilder();
		List<SelectItem> list = this.getSelectItems();
		
		
		for (int i=0; i<level; i++) {
			path.append("-");
		}
		path.append("Project[");
		for (SelectItem str : list) {
			path.append(str.toString()).append(",");	
		}
		path.deleteCharAt(path.length()-1);
		path.append("]");
		
		PhysicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}
	
}