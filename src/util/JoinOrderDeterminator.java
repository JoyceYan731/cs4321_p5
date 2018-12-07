package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.DataBase;
import data.PlanCostInfo;
import data.TableStat;
import data.UfCollection;
import data.UfElement;
import operators.Operator;
/**
 * This class is used to determine the join order, according to a list of tables. 
 * The order will be returned as a string of indexes of those tables.(the index of tables in the list)
 * @author Yixuan Jiang
 *
 */
public class JoinOrderDeterminator {
	private List<String> tableAliases;
	private Map<String, Integer> outputSizeMap;//Store the output size after selection, String: tableAliase
	private Map<String, UfElement> ufcMap;//The union-find map
	private Map<String, Set<String>> ufcDirec;//Store the table aliase and corresponding attrs in the ufcMap (with equality conditions)
	private Map<String, TableStat> statistics;
	private List<String> tableNames;
	private Map<String, Map<String, Long>> vMap;
	public JoinOrderDeterminator(List<String>tNames, List<String> tList, Map<String, Integer> oMap, UfCollection u, Map<String, Map<String, Long>> v) {
		tableAliases = tList;
		outputSizeMap = oMap;
		ufcMap = u.getMap();
		vMap = v;
		ufcDirec = new HashMap<String, Set<String>>();
		statistics = DataBase.getInstance().getStatistics();
		tableNames = tNames;
		for (String key : ufcMap.keySet()) {
			UfElement uEle = ufcMap.get(key);
			if(uEle.getAttributes().size() > 1) {
				String[] attr = key.split("\\.");
				if(!ufcDirec.containsKey(attr[0])) {
					Set<String> attrs = new HashSet<String>();
					attrs.add(key);
					ufcDirec.put(attr[0], attrs);
				}else {
					Set<String> attrs = ufcDirec.get(attr[0]);
					attrs.add(key);
					ufcDirec.put(attr[0], attrs);
				}
			}
			
		}
	}
	
	/*Get the join order and return in a string of the indexes of the tables*/
	public List<Integer> getOrder(){
		StringBuilder resKey = new StringBuilder();
		for (int i = 0; i < tableAliases.size(); i++) {
			resKey.append(String.valueOf(i));
		}
		List<HashSet<String>> subsets = new ArrayList<HashSet<String>>();
		for(int i = 0; i < tableAliases.size(); i++) {
			subsets.add(new HashSet<String>());
		}
		StringBuilder candSet = new StringBuilder();
		getAllSubsets(0, candSet, subsets);
		Map<String, PlanCostInfo> costMap = new HashMap<String, PlanCostInfo>();
		buildCostMap(costMap, subsets);
		String order = costMap.get(resKey.toString()).bestOrder;
		char[] orderChars = order.toCharArray();
		List<Integer>res = new LinkedList<Integer>();
		for (int i = 0; i < orderChars.length; i++) {
			Character ind = orderChars[i];
			res.add(Integer.parseInt(ind.toString()));
		}
		return res;
	}
	/*Get all the possible subsets of the tables using DFS*/
	private void getAllSubsets(int currInd, StringBuilder candSet, List<HashSet<String>> subsets){
		if(currInd == tableAliases.size()) {
			if (candSet.length() >0) {
				int index = candSet.length()-1;
				subsets.get(index).add(candSet.toString());
			}
			return;
		}
		candSet.append(String.valueOf(currInd));
		getAllSubsets(currInd + 1, candSet, subsets);
		candSet.deleteCharAt(candSet.length()-1);
		getAllSubsets(currInd + 1, candSet, subsets);
	}
	/*Building the cost map using buttom-up Dynamic Programming*/
	private void buildCostMap(Map<String, PlanCostInfo> costMap, List<HashSet<String>> subsets) {
		Map<String, Set<String>> tempDirc = this.ufcDirec;
		for (String key : ufcDirec.keySet()) {
			tempDirc.put(key, this.ufcDirec.get(key));
		}
		for (int i = 0; i < subsets.size(); i++) {
			HashSet<String> keys = subsets.get(i);
			for(String key : keys) {
				PlanCostInfo planCost = getCost(key, costMap, tempDirc);
				costMap.put(key, planCost);
			}
		}
	}
 
	/**
	 * Compute the cost of corresponding join plan
	 * @param tables: all the tables taken into consideration
	 * @param costMap: the map of the optimal cost of each plan, key: tables' indexes, value: optimal cost
	 * @param direc: Store the attributes with equality join conditions in current case, key: table aliases, value: set of attributes
	 * @return the cost information of this plan, including the cost, the output and the optimal join order.
	 */
	private PlanCostInfo getCost(String tables, Map<String, PlanCostInfo> costMap, Map<String,Set<String>> direc) {
		if (tables.length() == 0) return null;
		char[] tableChars = tables.toCharArray();
		if (tableChars.length == 1) {
			//There is only one table
			Character tableChar = tableChars[0];
			int tableIndex = Integer.parseInt(tableChar.toString());
			String tableAliase = tableAliases.get(tableIndex);
			int cost = 0;
			int outputSize = outputSizeMap.get(tableAliase);
			String order = String.valueOf(tableIndex);
			Set<String> allTables = new HashSet<String>();
			allTables.add(tableAliase);
			PlanCostInfo res = new PlanCostInfo(cost, outputSize, order, allTables, new HashMap<String, Long>(vMap.get(tableAliase)));
			return res;
		}else{
			int cost = -1;
			String order = "";
			String rightAliase = "";
			PlanCostInfo leftPlanCost = null;
			//Determine the optimal join order by compute the cost
			for (int i = 0; i < tableChars.length; i++) {
				StringBuilder sb = new StringBuilder(tables);
				String left = sb.deleteCharAt(i).toString();
				PlanCostInfo tempLeftPlanCost= costMap.get(left);
				int leftCost = tempLeftPlanCost.cost;
				int leftOutput = tempLeftPlanCost.outputSize;
				int currCost = leftCost +leftOutput;
				if (cost == -1 || currCost < cost) {
					cost = currCost;
					Character tableChar = tableChars[i];
					int rightIndex = Integer.parseInt(tableChar.toString());
					rightAliase = tableAliases.get(rightIndex); 
					order = sb.append(tableChar).toString();
					leftPlanCost = tempLeftPlanCost;
				}
			}
			//Determine the output size of this order
			Set<String> leftAllTables = leftPlanCost.allTables;
			Map<String, Long> leftVMap = new HashMap<String, Long>(leftPlanCost.vMap);
			Map<String, Long> rightVMap = new HashMap<String, Long>(vMap.get(rightAliase));

			int rightOutput = outputSizeMap.get(rightAliase);
			int leftOutput = leftPlanCost.outputSize;
			long denominator = 1;
			if(direc.containsKey(rightAliase)) {
				//There are equality conditions related to the right relation
				Set<String> rightEquaAttrs = direc.get(rightAliase);
				//List<String> rightRemoveList = new LinkedList<String>();
				//List<String> leftRemoveList = new LinkedList<String>();
				for(String rightEqualAttr : rightEquaAttrs) {
					UfElement uEle = ufcMap.get(rightEqualAttr);
					List<String> leftEqualAttrs = uEle.getAttributes();//All the attributes equal to this target right attribute
					long max = rightVMap.get(rightEqualAttr);
					long min = rightVMap.get(rightEqualAttr);
					List<String> currLeftAttributes = new LinkedList<String>();
					for (String leftEqualAttr : leftEqualAttrs) {
						String[] splittedAttr = leftEqualAttr.split("\\.");
						if (leftAllTables.contains(splittedAttr[0])) {
							//rightRemoveList.add(rightEqualAttr);
							//leftRemoveList.add(leftEqualAttr);
							currLeftAttributes.add(leftEqualAttr);
							if (leftVMap.get(leftEqualAttr) < min) {
								min = leftVMap.get(leftEqualAttr);
							}
							if (leftVMap.get(leftEqualAttr) > max){
								max = leftVMap.get(leftEqualAttr);
							}
						}
					}
					
					denominator = denominator * max;
					rightVMap.put(rightEqualAttr, min);
					for (String cLeftAttr : currLeftAttributes) {
						leftVMap.put(cLeftAttr, min);
					}
					
				}
//				if(denominator != 1) {
//					//There are equality conditions can be handled here 
//					for (String right : rightRemoveList) {
//						UfElement uEle = ufcMap.get(right);
//						String rightName = tableNames.get(tableAliases.indexOf(rightAliase));
//						TableStat rightStatistics = statistics.get(rightName);
//						String rightStatisticsKey = right.split("\\.")[1];
//						int attrInd  = rightStatistics.columns.indexOf(rightStatisticsKey);
//						List<Long> lBounds = rightStatistics.lowerBound;
//						List<Long> uBounds = rightStatistics.upperBound;
//						//If the upper bound in the union-find is null, there is no constraint on this attribute.
//						//The upper bound is the same as the original upper bound in the statistics. So is the lower bound.
//						Long rightLower = (uEle.getLowerBound() == null) ? lBounds.get(attrInd) : uEle.getLowerBound();
//						Long rightUpper = (uEle.getUpperBound() == null) ? uBounds.get(attrInd) : uEle.getUpperBound();
//						long vRight = Math.min(rightOutput, (rightUpper-rightLower+1));
//						long vLeft = Math.min(leftOutput, (rightUpper-rightLower+1));
//						denominator = denominator*Math.max(vRight, vLeft);
//						rightEquaAttrs.remove(right);
//						if(rightEquaAttrs.isEmpty()) {
//							direc.remove(rightAliase);
//						}else {
//							direc.put(rightAliase, rightEquaAttrs);
//						}
//					}
//				}
			}
			Set<String> allTables = new HashSet<String>();
			for (String table: leftAllTables) {
				allTables.add(table);
			}
			allTables.add(rightAliase);
			int outputSize = (int)(leftOutput*rightOutput/denominator);
			for (String rightKey : rightVMap.keySet()) {
				leftVMap.put(rightKey, rightVMap.get(rightKey));
			}
			PlanCostInfo res = new PlanCostInfo(cost, outputSize, order, allTables, leftVMap);
			return res;	
		}
	}
	

}
