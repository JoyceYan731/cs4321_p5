package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import data.DataBase;
import data.IndexNote;
import data.TableStat;
import data.UfCollection;
import data.UfElement;
import logicalOperators.LogicalScanOperator;

public class SelectDeterminator {
	/*Target logical scan oprator*/
	private LogicalScanOperator lScan;
	/*Store the index information related to this table, String: tableName.Attr*/
	private Map<String, IndexNote> indMap;
	/*The number of tuples in this table */
	private int t;
	/*The number of pages in this table*/
	private int p;
	/*Reduction factor map, String: tableAliase.Attr*/
	private Map<String, Float> redMap;
	/*Union find*/
	private UfCollection ufc;
	private String tableName;
	private String tableAliase;
	
	public SelectDeterminator(LogicalScanOperator ls, UfCollection u) {
		this.ufc = u;
		this.lScan = ls;
		this.indMap = new HashMap<String, IndexNote>();
		this.redMap = new HashMap<String, Float>();
		tableName = lScan.getTableName();
		tableAliase = lScan.getTableAliase();
		Map<String, List<IndexNote>> indexInfoRoster = DataBase.getInstance().getIndexInfos();
		if(indexInfoRoster.containsKey(tableName)) {
			List<IndexNote> indexedColumns = indexInfoRoster.get(tableName);
			for (IndexNote in : indexedColumns) {
//				StringBuilder sb = new StringBuilder();
//				sb.append(tableName);
//				sb.append(".");
//				sb.append(in.getColumn());
				indMap.put(in.getColumn(), in);
				
			}
		}
		Map<String, TableStat> statistics = DataBase.getInstance().getStatistics();
		TableStat tableStatistics = statistics.get(tableName);
		t = tableStatistics.tupleNumber;
		LinkedList<String> attrs = DataBase.getInstance().getSchema(tableName);
		int tupleSize = attrs.size()*4;
		p = t*tupleSize/4096;
		List<Long> lBounds = tableStatistics.lowerBound;
		List<Long> uBounds = tableStatistics.upperBound;
		Map<String, UfElement> ufMap = u.getMap();
		for (int i = 0; i < attrs.size(); i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(tableAliase);
			sb.append(".");
			sb.append(attrs.get(i));
			String key = sb.toString();
			long lBound = lBounds.get(i);
			long uBound = uBounds.get(i);
			if(ufMap.containsKey(key)) {
				UfElement uEle = ufMap.get(key);
				long lBound2 = (uEle.getLowerBound()==null) ? lBound : uEle.getLowerBound();
				long uBound2 = (uEle.getUpperBound()==null) ? uBound : uEle.getUpperBound();
				float reFactor = (uBound2-lBound2+1f)/(uBound-lBound+1f);
				redMap.put(key, reFactor);
			}else {
				float reFactor = (float)1.0;
				redMap.put(key, reFactor);
			}
		}
		
		
	}
	/**
	 * Determine which column to use
	 * @return If an index is used, corresponding attribute will be returned. If not to use index, return null.
	 */
	public String selectColumn() {
		String tableName = this.lScan.getTableName();
		Map<String, Integer> indexLeaves = DataBase.getInstance().getIndexLeaves();
		String res = null;
		float cost = p;
		for (String candCol : indMap.keySet()) {
			boolean isClustered = checkClustered(candCol);
			StringBuilder sb = new StringBuilder();
			sb.append(tableAliase);
			sb.append(".");
			sb.append(candCol);
			String key = sb.toString();
			float r = redMap.get(key);
			StringBuilder sb2 = new StringBuilder();
			sb2.append(tableName);
			sb2.append(".");
			sb2.append(candCol);
			String key2 = sb2.toString();
			int l = indexLeaves.get(key2);
			
			if (isClustered) {
				//if clustered, compute the cost
				float cost2 = 3 + p*r;
				if (cost2 < cost) {
					res = key;
					cost = cost2;
				}
			}else {
				// if unclustered, compute the cost
				float cost2 = 3 + l*r + t*r;
				if (cost2 < cost) {
					res = key;
					cost = cost2;
				}
			}
		}
		
		return res;
	}
	/**
	 * Check this column is clustered or not
	 * @param column (eg. Sailors.A)
	 * @return clustered or not
	 */
	public boolean checkClustered(String column) {
		if (column == null) return false;
		else if (indMap.containsKey(column)) {
			return indMap.get(column).isClustered();
		}
		return false;
	}
	public int computeOutputSize() {
		float reduction = 1;
		for (String s : redMap.keySet()) {
			reduction = reduction*redMap.get(s);
		}
		float res = t*reduction;
		return (int)res;
	} 
	

}
