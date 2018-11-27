package data;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 *This class provides function :
 *a catalog storing the information of the database.
 *The information includes:
 *where the table is and the table schema
 * 
 * @author Yixuan Jiang
 */

public class DataBase {
	
	/*Get the needed paths*/
	private String query_path = Dynamic_properties.queryPath;
	private String data_path = Dynamic_properties.dataPath;
	private String schema_path = Dynamic_properties.schemaPath;
	
	/* p3 update: get the path of index-info */
	private String indexInfo_path = Dynamic_properties.indexInfoPath;
    
	/* p4 update: the statistics info (TableStat) of a given table (String) is stored in the map */
    private Map<String, TableStat> statistics = new HashMap<>();
    /* store the number of leaf pages of indexed columns, String is tableName.attribute */
	private Map<String, Integer> indexLeaves; 
	
	
	/*Track the address of each table
	 * key:the name of the table
	 * value: the address of the table*/
	private Map<String, String> addresses = new HashMap<String, String>();
	
	/* Store the schemas of all the tables
	 * key: the name of each table
	 * value: table column names
	 */
	private Map<String, LinkedList<String>> schemas = new HashMap<String, LinkedList<String>>();
	
	/* p3 update: stores the information from index_info.txt */
	private Map<String, List<IndexNote>> indexInfoRoster = new HashMap<>();
	
	/*Singleton pattern*/
	private static volatile DataBase Instance =null;
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields
	 * 
	 */
	private DataBase() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(schema_path));
			String line = br.readLine();
			while(line !=null) {
				String[] res = line.split("\\s+");
				addresses.put(res[0], data_path+res[0]);
				LinkedList<String> columns = new LinkedList<String>();
				for (int i = 1; i<res.length; i++) {
					columns.add(res[i]);
				}
				schemas.put(res[0], columns);
				line = br.readLine();
			}
			br.close();
			
			BufferedReader brIndex = new BufferedReader(new FileReader(indexInfo_path));
			String lineIndex = brIndex.readLine();
			while(lineIndex !=null) {
				String[] res = lineIndex.split("\\s+");
				if (res.length == 4) {
					/*IndexNode : column, isClustered, order*/
					IndexNote indexInfo = new IndexNote(res[1], Integer.valueOf(res[2]) == 1, Integer.valueOf(res[3]));
					if(indexInfoRoster.containsKey(res[0])) {
						List<IndexNote> indexInfos = indexInfoRoster.get(res[0]);
						indexInfos.add(indexInfo);
						indexInfoRoster.put(res[0], indexInfos);
					}else {
						List<IndexNote> indexInfos = new ArrayList<IndexNote>();
						indexInfos.add(indexInfo);
						indexInfoRoster.put(res[0], indexInfos);
					}
				}
				lineIndex = brIndex.readLine();
			}
			brIndex.close();
			statistics = new HashMap<String, TableStat>();
			indexLeaves = new HashMap<String, Integer>();
			
		}catch(IOException e) {
			System.out.println(e.getMessage());
		}
		
	}
	
	/**
	 * get the instance of this Database
	 * @return a thread safe instance
	 */
	public static DataBase getInstance() {
		if (Instance == null) {
			synchronized (DataBase.class) {
				if (Instance == null) {
					Instance = new DataBase();
				}
				
			}
		}
		return Instance;
	}
	
	/**
	 * getter method to get address
	 */
	public String getAddresses(String str){
		return addresses.get(str);
	}
	
	/**
	 * getter method to get schema 
	 */

	public LinkedList<String> getSchema(String str){
		return schemas.get(str);
	}
	
	/**
	 * get info of index
	 */
	public Map<String, List<IndexNote>> getIndexInfos() {
		return indexInfoRoster;
	}
	
	/**
	 * get the statistics
	 */
	public Map<String, TableStat> getStatistics(){
		return statistics;
	}
	/**
	 * get the statistics
	 */
	public Map<String, Integer> getIndexLeaves(){
		return indexLeaves;
	}
	/**
	 * Update the statistics
	 */
	public void updateStatistics(String key, TableStat value) {
		this.statistics.put(key, value);
	}
	/**
	 * Update the indexLeaves
	 */
	public void updateIndexLeaves(String key, int value) {
		this.indexLeaves.put(key, value);
	}
	
}