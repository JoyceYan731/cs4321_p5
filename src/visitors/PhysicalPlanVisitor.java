package visitors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import data.Dynamic_properties;
import data.TablePair;
import data.UfCollection;
import logicalOperators.LogicalDuplicateEliminationOperator;
import logicalOperators.LogicalJoinOperator;
import logicalOperators.LogicalOperator;
import logicalOperators.LogicalProjectOperator;
import logicalOperators.LogicalScanOperator;
import logicalOperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.BNLJoinOperator;
import operators.DuplicateEliminationOperator;
import operators.InMemSortOperator;
import operators.IndexScanOperator;
import operators.JoinOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SMJoinOperator;
import operators.ScanOperator;
import operators.SortOperator;
import operators.V2ExternalSortOperator;
import util.GlobalLogger;
import util.JoinOrderDeterminator;
import util.SelectDeterminator;
/**
 * Visit the logical plan and construct a physical operator 
 * query plan
 * @author Yixuan Jiang
 *
 */
public class PhysicalPlanVisitor {
	private Operator root;
	private LinkedList<Operator> childList;
	private LinkedList<String> tableNames;//Store names of joined tables 
	private LinkedList<String> tableAliases;//Store aliases of joined tables
	private Map<String, Integer> outputSizeMap;//Store the output size after selection (in tuples)
	private int queryNum;
	//private int joinType=0; // 0: TNLJ, 1: BNLJ, 2: SMJ
	private int sortType=0; // 0: in-memory, 1: external
	//private int indexState=0; // 0: full-scan, 1: use indexes 
	private int bnljBufferSize = 5;//use 5 pages for BNLJ
	private int exSortBufferSize = 6;//use 6 pages for external sort
	private UfCollection ufc;
	//Constructor
	public PhysicalPlanVisitor() {
		this.childList = new LinkedList<Operator>();
	}
	public PhysicalPlanVisitor(int qN, UfCollection u) {
		this.childList = new LinkedList<Operator>();
		this.tableAliases = new LinkedList<String>();
		this.tableNames = new LinkedList<String>();
		this.outputSizeMap = new HashMap<String, Integer>();
		this.queryNum = qN;
		this.ufc = u;
	}

	
	/**
	 * Get the physical query plan
	 * @return the root operator of the physical plan
	 */
	public Operator getPhysicalRoot() {
		return root;
	}
	
	/**
	 * Once visit a logical scan oprator, construct a 
	 * physical scan operator and add it to the child list.
	 * A SelectSterminator return a string which is an indexed column that
	 * is used to build an indexed scan operator. If it returns null,
	 * just build a full scan operator.
	 * @param scOp: logical scan operator
	 */
	public void visit(LogicalScanOperator scOp) {
		String tableName = scOp.getTableName();
		String tableAliase = scOp.getTableAliase();
		Expression expression = scOp.getCondition();
		SelectDeterminator sd = new SelectDeterminator(scOp, this.ufc);
		String selectColumn = sd.selectColumn();
		boolean clustered = sd.checkClustered(selectColumn);
		int output = sd.computeOutputSize();
		if(selectColumn != null) {
			IndexExpressionVisitor indVisitor = new IndexExpressionVisitor(scOp, selectColumn);
			indVisitor.Classify();
			Integer[] bounds = indVisitor.getBounds();
			String column = indVisitor.getIndexColumn();
			Expression unindexedCondition = indVisitor.getUnIndexedCondition();
			ScanOperator scan = new ScanOperator(tableName, tableAliase, unindexedCondition);
			if(!(bounds[0]==null && bounds[1]==null)) {
				IndexScanOperator indScan = new IndexScanOperator(tableName, tableAliase, column, bounds[1], bounds[0], clustered);
				scan.setLeftChild(indScan);
			}
			childList.add(scan);
			tableNames.add(tableName);
			tableAliases.add(tableAliase);
			outputSizeMap.put(tableAliase, output);
			root = scan;
		}else {
			ScanOperator scan = new ScanOperator(tableName, tableAliase, expression);
			childList.add(scan);
			tableNames.add(tableName);
			tableAliases.add(tableAliase);
			outputSizeMap.put(tableAliase, output);
			root = scan;
		}
		
	}
	
	/**
	 * Once visit a logical join operator, visit its children. After the child list is updated, construct a 
	 * physical join operator. After determining the join order, generate join operators in this order. Update the child list.
	 * @param jnOp: logical join operator
	 * @throws Exception 
	 */
	public void visit(LogicalJoinOperator jnOp) {
		List<LogicalOperator> children = jnOp.getChildList();
		if (!children.isEmpty()) {
			for(LogicalOperator op : children) {
				op.accept(this);
			}
		}
		
		//Determine the join order
		JoinOrderDeterminator jd = new JoinOrderDeterminator(this.tableNames, this.tableAliases, this.outputSizeMap, this.ufc);
		List<Integer> joinOrder = jd.getOrder();
		LinkedList<Operator> tempChildList = new LinkedList<Operator>();
		Set<String> tempAllTable = new HashSet<String>();
		Map<TablePair, Expression> joinConditions = jnOp.getjoinConditions();
		int leftMostInd = joinOrder.get(0);
		if(joinOrder.size() > 1) {
			for(int i = 1; i < joinOrder.size(); i++) {
				int rightInd = joinOrder.get(i);
				if (tempChildList.isEmpty()) {
					Operator left = childList.get(leftMostInd);
					Operator right = childList.get(rightInd);
					String leftAlise = tableAliases.get(leftMostInd);
					String rightAliase = tableAliases.get(rightInd);
					tempAllTable.add(rightAliase);
					tempAllTable.add(leftAlise);
					Expression expr = findExpression(joinConditions, tempAllTable);
					tempChildList.add(generatePhysicalJoin(left, right, expr));
					 
				}else {
					Operator left = tempChildList.pollLast();
					Operator right = childList.get(rightInd);
					String rightAliase = tableAliases.get(rightInd);
					tempAllTable.add(rightAliase);
					Expression expr = findExpression(joinConditions, tempAllTable);
					tempChildList.add(generatePhysicalJoin(left, right, expr));
				}
			}
		}else {
			tempChildList.add(childList.pollLast());
		}
		
		for (int i = 0; i < childList.size(); i++) {
			childList.remove();
		}
		Operator join= tempChildList.pollLast();
		childList.add(join);
		root = join;
	
	}
	
	
	/*Get the expression according to allTables, and update joinConditions*/
	private Expression findExpression(Map<TablePair, Expression> joinConditions, Set<String> allTables) {
		Expression expr = null;
		HashSet<TablePair> removeList = new HashSet<TablePair>();
		for(TablePair tbpir: joinConditions.keySet()) {
			if (allTables.contains(tbpir.first()) && allTables.contains(tbpir.second())) {
				if (expr == null) {
					expr = joinConditions.get(tbpir);
				} else {
					expr = new AndExpression(expr, joinConditions.get(tbpir));
				}
			    removeList.add(tbpir);
			}
		}
		for (TablePair tbpir: removeList) {
			joinConditions.remove(tbpir);
		}
		return expr;
		
	}
	
	/*If the expression is an equality condition, return a SMJoinOperator. Otherwise, return a BNLJOperator*/
	private Operator generatePhysicalJoin(Operator left, Operator right, Expression expr) {
		EqualityExpressionVisitor eev = new EqualityExpressionVisitor();
		if(expr == null) {
			BNLJoinOperator join = new BNLJoinOperator(left, right, expr, bnljBufferSize);
			return join;
		}
		expr.accept(eev);
		if (eev.isEqal()) {
			SMJoinOperator join = new SMJoinOperator(left, right, expr);
			if (left != null) {
				Operator originalLeft = left;
				//left = new ExternalSortOperator(queryNum, exSortBufferSize, join3.getLeftSortColumns(), originalLeft.getSchema(), originalLeft);
				left = new V2ExternalSortOperator(queryNum, exSortBufferSize, join.getLeftSortColumns(), originalLeft.getSchema(), originalLeft);
			}
			if (right != null) {
				Operator originalRight = right;
				//right = new ExternalSortOperator(queryNum, exSortBufferSize, join3.getRightSortColumns(), originalRight.getSchema(), originalRight);
				right = new V2ExternalSortOperator(queryNum, exSortBufferSize, join.getRightSortColumns(), originalRight.getSchema(), originalRight);		

			}
			join.setLeftChild(left);
			join.setRightChild(right);
			return join;
		}else {
			BNLJoinOperator join = new BNLJoinOperator(left, right, expr, bnljBufferSize);
			return join;
		}
	}
	
	/**
	 * Once visit a logical project operator, visit its left child. After
	 * the child list is updated, construct a physical project operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical project operator
	 */
	public void visit(LogicalProjectOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		List<SelectItem> sI = operator.getSelectItems();
		Operator left = childList.pollLast();
		ProjectOperator project = new ProjectOperator(sI, left, operator.getAliasOrder());
		childList.add(project);
		root = project;
	}
	
	/**
	 * Once visit a logical sort operator, visit its left child. After
	 * the child list is updated, construct a physical sort operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical sort operator
	 */
	public void visit(LogicalSortOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollLast();
		if (sortType == 0) {
			SortOperator sort1 = new SortOperator(sI, left);
			childList.add(sort1);
			root = sort1;
		}else if (sortType == 1) {
			List<OrderByElement> list = sI.getOrderByElements();
			List<String> attributes = new LinkedList<String>();
			if(list != null) {
				for (int i=0; i<list.size(); i++) {
					attributes.add(list.get(i).toString());
				}
			}
			V2ExternalSortOperator sort2;
			sort2 = new V2ExternalSortOperator(queryNum, exSortBufferSize, attributes, left.getSchema(), left);
			childList.add(sort2);
			root = sort2;
			
		}
		
	}
	
	/**
	 * Once visit a logical logical DuplicateElimination operator, visit its left child. After
	 * the child list is updated, construct a physical DuplicateElimination operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical DuplicateElimination operator
	 */
	public void visit(LogicalDuplicateEliminationOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollLast();
		DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(sI, left);
		
		childList.add(distinct);
		root = distinct;
	}

}
