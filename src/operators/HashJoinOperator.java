package operators;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import data.Dynamic_properties;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.PhysicalLogger;
import util.TupleReader;
import util.TupleWriter;
import visitors.LocateExpressionVisitor;

/**
 * This class implements Hash Join:
 * 
 * There are three phases for Hash join 
 * 
 * (1) Partitioning Phase: given two tables R and S, partitioning two tables separately and save temporary 
 * partition file on disk
 * (2) Building Phase: building hash table according to the hash key of small table, say R table, for each partition
 * (3) Probing Phase: probing S table using hash function and adding concatenated tuple to message queue,
 * get the next tuple from message queue
 * 
 * @author Xiaoxing Yan
 */



public class HashJoinOperator extends JoinOperator{


	/* define the bucket size used in two hash functions*/
	private static final int PARTITION_BUCKET_SIZE = 10;
	private static final int BUILDING_BUCKET_SIZE = 50;

	/* fetch tuples from messageQueue*/
	private Queue<Tuple> messageQueue;
	/* the maximum size for messageQueue*/
	private static final int MAX_QUEUE_SIZE = 1500;
	/* the maximum number of threads which will be active to process tasks */
	private static final int MAX_THREAD_NUMBER = 8;
	/* the number of thread which has finished */
	private int threadCount;

	/* store temporary file of partition*/
	private Map<Integer, TupleWriter> leftBucketWriters;
	private Map<Integer, TupleWriter> rightBucketWriters;

	/* store attributes in equality condition*/
	private List<String> leftRelationColumns;
	private List<String> rightRelationColumns;

	/* schema for tuples from left relation and right relation respectively */
	private Map<String, Integer> leftSchema;
	private Map<String, Integer> rightSchema;

	/* address for temporary file*/
	private String leftFileAddress = Dynamic_properties.tempPath +"/"+ "hash_left_"+ this.hashCode()+"_";
	private String rightFileAddress = Dynamic_properties.tempPath +"/"+ "hash_right_"+ this.hashCode()+"_";


	/**
	 * Constructor for HashJoin Operator
	 * 
	 * @param op1
	 * @param op2
	 * @param expression
	 */
	public HashJoinOperator(Operator op1, Operator op2, Expression expression) {
		super(op1, op2, expression);

		StringBuilder sb = new StringBuilder();
		sb.append("hashjoin-");
		sb.append(op1.name);
		sb.append("-");
		sb.append(op2.name);
		name = sb.toString();

		/* extract the columns related to op1 from expression
		 * extract the columns related to op2 from expression, corresponding to sequence of op1
		 */
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftRelationColumns = locator.leftSortColumns();
			rightRelationColumns = locator.rightSortColumns();
		}

		messageQueue = new LinkedList<>();

		try {
			partitionPhase();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}



	@Override
	public Tuple getNextTuple() {

		Tuple cur = null;
		try {
			synchronized (messageQueue) {

				while (messageQueue.isEmpty()) {
					/* case 1: already read all matched tuple*/
					if (threadCount == PARTITION_BUCKET_SIZE) {
						return null;
					} else {
						/* case 2: thread has not provided new tuple*/	
						messageQueue.wait();
					}
				}

				cur = messageQueue.poll();
				messageQueue.notify();
			}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			return cur;
		}

	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	/**
	 * partition phase for hash join
	 * 
	 * @throws IOException
	 */
	public void partitionPhase () throws IOException {

		leftBucketWriters = new HashMap<>();
		rightBucketWriters = new HashMap<>();

		/* initialize partitioning buckets*/
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {

			String tempAddress = leftFileAddress + i;
			TupleWriter tupleWriter1 = new TupleWriter(tempAddress);
			leftBucketWriters.put(i, tupleWriter1);

			String tempAddress2 = rightFileAddress + i;
			TupleWriter tupleWriter2 = new TupleWriter(tempAddress2);
			rightBucketWriters.put(i, tupleWriter2);
		}

		/* read tuples from left and right relation respectively*/
		Tuple left = this.leftChild.getNextTuple();
		leftSchema = left.getSchema();
		while (left != null) {
			int hashcode = leftHashCode1(left);
			leftBucketWriters.get(hashcode).writeTuple(left);	
			left = this.leftChild.getNextTuple();
		}
	

		Tuple right = this.rightChild.getNextTuple();
		rightSchema = right.getSchema();
		while (right != null) {
			int hashcode = rightHashCode1(right);
			rightBucketWriters.get(hashcode).writeTuple(right);
			right = this.rightChild.getNextTuple();
		}
		
		/* close files */
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {
			leftBucketWriters.get(i).writeTuple(null);
			rightBucketWriters.get(i).writeTuple(null);
		}


		try {
			BuildingPhase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Building hash table and find matched tuple for each partition
	 * 
	 * @throws Exception 
	 */
	public void BuildingPhase() throws Exception {

		/* start parallel join process*/
		ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREAD_NUMBER);
		threadCount = 0;
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {
			threadPool.execute(new Task(i));
		}
		
		threadPool.shutdown();

	}


	/**
	 * Task for multiple threads
	 */
	class Task implements Runnable{

		private int threadNumber;
		private TupleReader leftReader;
		private TupleReader rightReader;

		private Map<Integer, List<Tuple>> hashTable;

		public Task(int i) {

			this.threadNumber = i;

			String leftAddress = leftFileAddress + i;
			leftReader = new TupleReader(leftAddress, leftSchema);
			String rightAddress = rightFileAddress + i;
			rightReader = new TupleReader(rightAddress, rightSchema);

			hashTable = new HashMap<>();

			/* assume that the memory is enough to load all data in this partition*/
			for (int j = 0; j < BUILDING_BUCKET_SIZE; j++) {
				hashTable.put(j, new ArrayList<>());
			}

			/* building hash table*/
			try {
				Tuple tuple = leftReader.readNextTuple();
				while (tuple != null) {
					hashTable.get(leftHashCode2(tuple)).add(tuple);
					tuple = leftReader.readNextTuple();
				}
				leftReader.close();
			}

			catch (Exception e) {
				e.printStackTrace();
			}

		}

		@Override
		public void run () {

			try {

				//System.out.println(Thread.currentThread().getName() + " is using");

				Tuple rightTuple = rightReader.readNextTuple();
				while (rightTuple != null) {
					int hash = rightHashCode2(rightTuple);

					/* hash value does not exist*/
					if (hashTable.get(hash) == null) {
						rightTuple = rightReader.readNextTuple();
						continue;
					}

					/* evaluate join condition for every pair of tuples*/
					for (Tuple leftTuple : hashTable.get(hash)) {
						boolean match = true;
						for (int k = 0; k < leftRelationColumns.size(); k++) {
							int index1 = leftTuple.getSchema().get(leftRelationColumns.get(k));
							long data1 = leftTuple.getData()[index1];

							int index2 = rightTuple.getSchema().get(rightRelationColumns.get(k));
							long data2 = rightTuple.getData()[index2];

							if (data1 != data2) {
								match = false;
								break;
							}


						}
						/* if two tuples cannot match, moving to the next pair*/
						if (!match) {
							continue;
						}

						/* add new tuple to the message queue*/
						Tuple newTuple = concatenate(leftTuple, rightTuple);

						synchronized (messageQueue) {
							/* case 1: when the messageQueue is full*/
							while (messageQueue.size() >=  MAX_QUEUE_SIZE) {
								messageQueue.wait();
							}

							/* case 2: messageQueue can accept new tuple
							 * then current thread will release the object lock
							 */
							messageQueue.offer(newTuple);
							messageQueue.notify();
						}
					}

					rightTuple = rightReader.readNextTuple();
				}



			}

			catch (Exception e) {
				e.printStackTrace();
			}

			finally {

				/* case: already read all tuples in this partition */
				synchronized (messageQueue) {
					threadCount ++;

					/* delete partition file*/
					String leftAddress = leftFileAddress + this.threadNumber;
					String rightAddress = rightFileAddress + this.threadNumber;

					File leftFile = new File(leftAddress);
					File rightFile = new File(rightAddress);
					leftFile.delete();
					rightFile.delete();

					File leftFile1 = new File(leftAddress+"_humanreadable");
					File rightFile1 = new File(rightAddress+"_humanreadable");
					leftFile1.delete();
					rightFile1.delete();

					messageQueue.notify();
				}

			}

		}

	}


	private int leftHashCode1(Tuple tuple) {
		int sum = 0;
		int index = tuple.getSchema().get(leftRelationColumns.get(0));
		long data = tuple.getData()[index];
		sum += data;

		//		for (String col : leftRelationColumns) {
		//			int index = tuple.getSchema().get(col);
		//			long data = tuple.getData()[index];
		//			sum += data;
		//		}
		return sum % PARTITION_BUCKET_SIZE;
	}

	private int rightHashCode1(Tuple tuple) {
		int sum = 0;
		int index = tuple.getSchema().get(rightRelationColumns.get(0));
		long data = tuple.getData()[index];
		sum += data;
		//		for (String col : rightRelationColumns) {
		//			int index = tuple.getSchema().get(col);
		//			long data = tuple.getData()[index];
		//			sum += data;
		//		}
		return sum % PARTITION_BUCKET_SIZE;
	}

	private int leftHashCode2(Tuple tuple) {
		int sum = 0;
		for (String col : leftRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % BUILDING_BUCKET_SIZE;
	}

	private int rightHashCode2(Tuple tuple) {
		int sum = 0;
		for (String col : rightRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % BUILDING_BUCKET_SIZE;
	}


	@Override
	public void printPlan(int level) {

		StringBuilder path = new StringBuilder();
		Expression exp = this.getExpression();

		/* print join line*/
		for (int i=0; i<level; i++) {
			path.append("-");
		}

		path.append("Hashjoin");
		if ( exp != null) {
			path.append("[");
			path.append(exp.toString());
			path.append("]");
		}

		PhysicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}



}
