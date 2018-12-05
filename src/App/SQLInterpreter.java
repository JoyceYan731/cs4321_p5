package App;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import data.DataBase;
import data.Dynamic_properties;
import data.IndexNote;
import data.TableStat;
import data.Tuple;
import logicalOperators.LogicalOperator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import util.GlobalLogger;
import util.LogicalLogger;
import util.PhysicalLogger;
import util.TupleReader;
import visitors.PhysicalPlanVisitor;
import logicalOperators.LogicalJoinOperator;


/**
 * This class provides function:
 * 
 * Interpret query to query plan
 * evaluate query plan and generate output files
 * 
 * @author Xiaoxing Yan
 *
 */
public class SQLInterpreter {

	/**
	 * Set paths according to input parameters
	 * 
	 * @param args   absolute paths
	 * @param args[0]   absolute path of input file
	 * @param args[1]   absolute path of output file
	 * @param args[2]   absolute path of temporary scratch file directory
	 * 
	 * @param args	configuration file path
	 * @param args[0]	path to configuration file
	 */
	public static int init (String[] args) {
		if(args.length==3) {
			Dynamic_properties.setPath(args[0], args[1], args[2]);
			try {
				IndexTreeBuilder itb = new IndexTreeBuilder();
				Map<String, List<IndexNote>> indexInfoRoster = DataBase.getInstance().getIndexInfos();
				itb.build();
				itb.sortRelations();
			}catch(Exception e) {
				e.printStackTrace();
			}
			return 1;
		}else if (args.length == 2) {
			Dynamic_properties.setPath(args[0], args[1]);
			return 1;
		}else if (args.length == 1) {
			File configuration = new File (args[0]);
			try {
				BufferedReader br = new BufferedReader(new FileReader(configuration));
				String inputDir = br.readLine();
				String outputDir = br.readLine();
				String tempDir = br.readLine();
				Dynamic_properties.setPath(inputDir, outputDir, tempDir);
				IndexTreeBuilder itb = new IndexTreeBuilder();
				Map<String, List<IndexNote>> indexInfoRoster = DataBase.getInstance().getIndexInfos();
				itb.build();
				itb.sortRelations();

				br.close();
				//return queryState;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 1;

	}

	/**
	 * get statistics of all relations in db.
	 */
	public static void statisticsCalculation() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(Dynamic_properties.schemaPath));
			String line = br.readLine();
			while(line !=null) {
				String[] res = line.split("\\s+");
				String tableInfo = res[0];
				List<String> columns = new LinkedList<String>();
				for (int i = 1; i<res.length; i++) {
					columns.add(res[i]);
				}
				TableStat tableSt = new TableStat(tableInfo, columns);
				TupleReader tr = new TupleReader(tableInfo);
				Tuple tp;
				int numTuple = 0;
				while((tp = tr.readNextTuple()) != null) {
					numTuple++;
					long[] data = tp.getData();
					for (int i = 0; i < data.length; i++) {
						if (tableSt.lowerBound.size() < i + 1) {
							tableSt.lowerBound.add(data[i]);
						} else {
							long val = Math.min(tableSt.lowerBound.get(i), data[i]);
							tableSt.lowerBound.set(i, val);
						}

						if (tableSt.upperBound.size() < i + 1) {
							tableSt.upperBound.add(data[i]);
						} else {
							long val = Math.max(tableSt.upperBound.get(i), data[i]);
							tableSt.upperBound.set(i, val);
						}
					}
				}
				tableSt.tupleNumber = numTuple;
				DataBase.getInstance().getStatistics().put(tableInfo, tableSt);
				line = br.readLine();
			}
			br.close();
			writeToDataBase(DataBase.getInstance().getStatistics());
		}catch(IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param statistics
	 */
	private static void writeToDataBase(Map<String, TableStat> statistics) {
		// Initialize the path of stats.txt
		StringBuilder statPath = new StringBuilder(Dynamic_properties.inputPath);
		statPath.append("/db/stats.txt");
		File statsFile = new File(statPath.toString());
		// if statsFile not exist, create it;
		// if it exists already, delete its content;
		if (!statsFile.exists()) {
			try {
				statsFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		statsFile.delete();

		// Initialize a buffer writer, use it to write statistics, then close it.
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(statsFile));
			for (TableStat tbst : statistics.values()) {
				bw.write(tbst.toString() + '\n');
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Build query tree for every query
	 */
	public static void BuildQueryPlan () {

		String queriesFile = Dynamic_properties.queryPath;
		try {
			Operator root = null;
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			net.sf.jsqlparser.statement.Statement statement;
			int index = 1;

			while ((statement = parser.Statement()) != null) {

				/*calculate spend time*/
				long startTime=System.currentTimeMillis(); 
				GlobalLogger.getLogger().info("\n");
				GlobalLogger.getLogger().info("\n");
				GlobalLogger.getLogger().info("Query: NO. " + index);
				GlobalLogger.getLogger().info("TIME START " + startTime);
				long endTime = 0;



				GlobalLogger.getLogger().info("Read statement: " + statement);

				//System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				LogicalPlanBuilder lb = new LogicalPlanBuilder(select);
				lb.buildLogicQueryPlan();


				/* print logical plan*/
				LogicalOperator treeRoot = lb.getRoot();
				writeLogicalPlan (treeRoot);




				PhysicalPlanVisitor pv = new PhysicalPlanVisitor(index, lb.getUfCollection());
				try {
					LogicalOperator lOp = lb.getRoot();
					lOp.accept(pv);
					root = pv.getPhysicalRoot();

					/* print logical plan*/

					writePhysicalPlan (root);

					/*get the ending time*/
					endTime=System.currentTimeMillis();  
					writeToFile (index, root);
					GlobalLogger.getLogger().info("time spent： "+(endTime - startTime)+"ms"); 


				} catch (Exception e) {
					//System.err.println("Exception occurred during paring query" + index);
					GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
					e.printStackTrace();
				}
				index++;	
			}

		} catch (Exception e){
			//System.err.println("Exception occurred during parsing");
			GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
			e.printStackTrace();
		}

	}

	/**
	 * write output to output files
	 * 
	 * @param index index of query
	 * @param root  root node of query tree
	 * @throws IOException 
	 */

	public static void writeToFile (int index, Operator root) throws IOException {
		root.dump(index);
		GlobalLogger.getLogger().info("end");
		//System.out.println("end");
	}







	public static void writeLogicalPlan (LogicalOperator treeRoot) {
		String str = "**************************************Query************************************************";
		LogicalLogger.getLogger().log(Level.SEVERE, str, new Exception());
		LogicalLogger.getLogger().log(Level.SEVERE, "\n", new Exception());
		writeLogicalPlanHelper (treeRoot, 0);
		LogicalLogger.getLogger().log(Level.SEVERE, "\n", new Exception());

	}

	public static void writeLogicalPlanHelper (LogicalOperator treeRoot, int level) {

		if (treeRoot == null) {
			return;
		}

		treeRoot.printPlan(level);

		if (treeRoot instanceof LogicalJoinOperator) {
			List<LogicalOperator> children = ((LogicalJoinOperator) treeRoot).getChildList();
			for (LogicalOperator child : children) {
				writeLogicalPlanHelper (child, level+1);
			}
		} else {
			if (treeRoot.getLeftChild() != null) {
				writeLogicalPlanHelper (treeRoot.getLeftChild(), level+1);
			}
			if (treeRoot.getRightChild() != null) {
				writeLogicalPlanHelper (treeRoot.getRightChild(), level+1);	
			}

		}



	}



	public static void writePhysicalPlan (Operator treeRoot) {
		String str = "**************************************Physical Plan************************************************";
		PhysicalLogger.getLogger().log(Level.SEVERE, str, new Exception());
		PhysicalLogger.getLogger().log(Level.SEVERE, "\n", new Exception());
		writePhysicalPlanHelper (treeRoot, 0);
		LogicalLogger.getLogger().log(Level.SEVERE, "\n", new Exception());

	}

	public static void writePhysicalPlanHelper (Operator treeRoot, int level) {
		if (treeRoot == null) {
			return;
		}

		treeRoot.printPlan(level);

		if (treeRoot.getLeftChild() != null) {
			writePhysicalPlanHelper(treeRoot.getLeftChild(), level+1);
		}
		if (treeRoot.getRightChild() != null) {
			writePhysicalPlanHelper (treeRoot.getRightChild(), level+1);	
		}


	}



}
