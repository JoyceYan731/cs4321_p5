package operators;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import data.Dynamic_properties;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.PhysicalLogger;
import util.TupleReader;
import util.TupleWriter;

public class V2ExternalSortOperator extends Operator{
	private int queryNum;
	private String tempAddress;
	private File tempDir;
	private File[] scratchFiles; // Current scratch files under the temporary directory 
	private TupleReader[] trs;
	//private boolean[] trsStates;
	private TupleWriter tw;
	private int bufferSize;
	private Tuple[] sortBuffer;
	private static int pageSize = 4096;
	private int tuplePerPage;
	//private Tuple[] outputPage;
	private List<String> attrList; // Records the comparing order
	private int passNum = 0;
	private int[] mergePointers; //Records the pointers of merge sort
	//private int outputPointer = 0;
	public V2ExternalSortOperator(int queryNumber, int bSize, List<String>attributes, Map<String, Integer>schema, Operator op) {
		this.leftChild = op;
		this.queryNum = queryNumber;
		this.bufferSize = bSize;
		this.schema = schema;
		int attrNum = this.schema.size();
		this.tuplePerPage = pageSize/(4*attrNum);
		int maxTuple = (bufferSize-1)*tuplePerPage;
		this.sortBuffer = new Tuple[maxTuple];
		this.trs = new TupleReader[bufferSize-1];
		//this.trsStates = new boolean[bufferSize-1];
		//this.outputPage = new Tuple[tuplePerPage];
		StringBuilder sb = new StringBuilder();
		sb.append("exSort-");
		sb.append(op.name);
		sb.append("-");
		sb.append(queryNum);
		name = sb.toString();
		this.tempAddress = Dynamic_properties.tempPath + "/external-sort/" + name;
		this.tempDir = new File(tempAddress);
		if(!tempDir.exists()) {
			tempDir.mkdirs();
		}
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
		this.attrList = attributes;
		this.mergePointers = new int[bufferSize-1];
		cleanTempDirectory();
		//initMergePointers();
		try {
			readSortWrite();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String fileName = scratchFiles[0].getName().replace("_humanreadable", "");	
		String fileAddress = getFileAddress(fileName);
		trs[0] = new TupleReader(fileAddress, this.schema);
		
	}
	
	/**
	 * Clean up the temp directory between queries
	 */
	public void cleanTempDirectory() {
		File[] scratchDirs = new File(Dynamic_properties.tempPath + "/external-sort").listFiles((dir, name) -> !name.equals(".DS_Store"));
		if (scratchDirs.length!=0) {
			//the first file is a human readable file
			for(File dir : scratchDirs) {
				String[] temp = dir.getName().split("-");
				
				String currQueryNum = temp[temp.length-1];
				if (!currQueryNum.equals(String.valueOf(queryNum)) ) {
					deleteDirectory(dir);
				}
			}
			
		}
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
	}
	private void deleteDirectory(File dir) {
		File[] contents = dir.listFiles();
		if (contents != null) {
			for (File f: contents) {
				deleteDirectory(f);
			}
		}
		dir.delete();
	}
	
	private void readSortWrite() throws Exception {
		/** pass 0 */
		int readState = 1;
		int fileNum = 0;
		while(readState == 1) {
			readState = readInBuffer();
			Arrays.sort(sortBuffer, new TupleComparator(this.attrList));
			String scratchPath = generatePath(fileNum);
			TupleWriter writer = new TupleWriter(scratchPath);//Needs a null tuple to close
			boolean f = true;
			for(Tuple t: sortBuffer) {
				writer.writeTuple(t);
				if(t == null) {
					f = false;
					break;
				}
			}
			if (f) {
				writer.writeTuple(null);
			}
			fileNum ++;
		}
		
		/** After pass 0 */
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store")); // 表示当前文件夹存有的所有file.
		// scratchFiles 作为全局变量，必须改？？
		Arrays.sort(this.scratchFiles);
		passNum = 1;
		fileNum = 0;
		// break point of a pass, MOST LIKELY TO PARALLEL COMPUTING IN THE BELOW PHASE	
		while (scratchFiles.length > 2) { // 因为scratchFiles包括readable file，所以以大于2为分界线！	
			int fileCountsBeforePass = scratchFiles.length;
			List<File> deleteFiles = new LinkedList<File>();
			//Execute a pass
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////			
			for (int i = 0; i < scratchFiles.length; i += 2 * (bufferSize - 1)) {  // i到底加多少呢？scratchFiles里面包不包括 _humanreadable呢？可以不包括啊！！
				TupleReader[] IOkeys = new TupleReader[bufferSize - 1]; // when initialized, every element in TupleReader array is null.

				for (int j = 0; j < IOkeys.length && (i + 2 * j) < scratchFiles.length; j++) {
					String fileName = scratchFiles[i + 2 * j].getName().replace("_humanreadable", "");
					int filePassNum = getFilePassNum(fileName);
					if (filePassNum < passNum) {  // 我真的需要这个判断吗？从scratch file拿出来的难道不是绝对是上一轮的passNum吗？不然我为何还要两个list？
						deleteFiles.add(new File(getFileAddress(fileName)));
						deleteFiles.add(new File(getFileAddress(fileName + "_humanreadable")));
						IOkeys[j] = new TupleReader(getFileAddress(fileName), this.schema); // 最关键：建立好了新的IO接口来读每个page的数据。
					} 
				}

				/* update fileNum if other B pages are loaded into buffer and merge for a new file*/
				fileNum = i / (2 * (bufferSize - 1));
				String scratchPath = generatePath(fileNum);
				TupleWriter currPassTpWt = new TupleWriter(scratchPath);
				
				/*Construct a local sort buffer to store pages
				 * Construct a local merge pointer to store the indexes on the pages*/
				Tuple[] localSortBuffer = new Tuple[(bufferSize - 1) * tuplePerPage];
				int[] localMergePointers = new int[bufferSize - 1];
				
				// load page into local buffer, modify the merge pointers meanwhile.
				for (int k = 0; k < IOkeys.length; k++) {
					// load full page content into the kth page via kth tupleReader
					int loadPageI = readInBuffer(k, IOkeys, localSortBuffer);
					if (loadPageI == 1) {
						localMergePointers[k] = k * tuplePerPage; // mergePoints records the starting point of merge
					}else {
						localMergePointers[k] = -1;
					} 
				}
				Tuple tuple = mergeSort(localMergePointers, IOkeys, localSortBuffer);
				while(tuple != null) {
					currPassTpWt.writeTuple(tuple);
					tuple = mergeSort(localMergePointers, IOkeys, localSortBuffer);
				}
				currPassTpWt.writeTuple(null);
			}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////			
			
			//Delete all the files of this last pass
			for (File f: deleteFiles) {
				f.delete();
			}			
			// update pass number, reset the scratchFiles.
			passNum++;
			this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
			Arrays.sort(this.scratchFiles);
			fileNum = 0;
		}
		File finalFile = scratchFiles[0];
		trs[0] = new TupleReader(finalFile.getPath(), this.schema);
	}
			
			
//			/* For every file in the scratchFiles list, put them into B tuple readers 
//			 * every time until all of them are all processed. */
//			int i = 0;//index of scratch files
//			while(i < fileCountsBeforePass) { // i is  the total num of files which have been processed in the new pass.
//				//Construct the tuple readers								
//				int j = 0; // j is the index of tuple readers				
//				while (j < trs.length) {
//					//if (j >= scratchFiles.length) break;
//					String fileName = scratchFiles[i].getName().replace("_humanreadable", "");
//					int filePassNum = getFilePassNum(fileName);
//					if (filePassNum < passNum) {
//						//Delete files of last pass
//						String fileAddress1 = getFileAddress(fileName);
//						String fileAddress2 = fileAddress1 + "_humanreadable";
//						deleteFiles.add(new File(fileAddress1));
//						deleteFiles.add(new File(fileAddress2));
//						trs[j] = new TupleReader(fileAddress1, this.schema);
//						//trsStates[j] = true;
//						j++;
//					}
//					i = i + 2;
//					if (i >= fileCountsBeforePass) {
//						while (j < trs.length) {
//							trs[j] = null;
//							j++;
//						}
//						break;
//					}
//				}
//				
//				String scratchPath = generatePath(fileNum);
//				tw = new TupleWriter(scratchPath);
//				for (int k = 0; k < trs.length; k++) {
//					// load full page content into the kth page via kth tupleReader
//					int loadPageI = readInBuffer(k);
//					if (loadPageI == 1) {
//						mergePointers[k] = k * tuplePerPage; // mergePoints records the starting point of merge
//					}else {
//						mergePointers[k] = -1;
//					} 
//				}
//				Tuple tuple = mergeSort();
//				int count = 0;
//				while(tuple != null) {
//					tw.writeTuple(tuple);
//					count ++;
//					tuple = mergeSort();
//				}
//				tw.writeTuple(null);
//				//System.out.println(count);
//				fileNum++;
//				//this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
//			}
//			//Delete all the files of this last pass
//			for (File f: deleteFiles) {
//				f.delete();
//			}
//			passNum++;
//			this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
//			Arrays.sort(this.scratchFiles);
//			fileNum = 0;
//		}
//	}
	
	/**
	 * this function is to fill all pages in sortBuffer to be null, and then read all the tuples into the sortBuffer from its left
	 * child. This function is used for pass 0 to flow all data into the memory, sort them and write out, without considering which 
	 * file to get the data from. (because the beginning point is to read data from child, not file)
	 * @return 0 if there is no tuple any longer; 1 if there are still tuples to put in.
	 * @throws Exception
	 */
	
	// critical session
	private int readInBuffer() throws Exception {
		//Clear the sort buffer
		Arrays.fill(this.sortBuffer, null);
		//Read in
		Tuple tuple = this.leftChild.getNextTuple();
		int i = 0;
		while(tuple != null && i < sortBuffer.length-1) {
			sortBuffer[i] = tuple;
			tuple = this.leftChild.getNextTuple();
			i++;
		}
		if (tuple == null) {
			return 0;
		}else {
			sortBuffer[i] = tuple;
			return 1;
		}
	}
	
	/**
	 * read the ith file in the list of "deletedFiles" via the ith tuple reader
	 * @param i
	 * @return
	 * @throws Exception
	 */
	private int readInBuffer(int i, TupleReader[] IOKeys, Tuple[] localSortBuffer) throws Exception {
		//Clear ith page
		for (int k = i * tuplePerPage; k < (i + 1) * tuplePerPage; k++) {
			localSortBuffer[k] = null;
		}
		
		if (i < IOKeys.length && IOKeys[i] != null) {
			int tupNum = 0;
			Tuple tuple = IOKeys[i].readNextTuple();
			while(tuple != null && tupNum < tuplePerPage - 1) {
				int index = i * tuplePerPage + tupNum;
				localSortBuffer[index] = tuple;
				tuple = IOKeys[i].readNextTuple();
				tupNum++;
			}
			if (tupNum == 0) return 0;  // tuple这个时候有可能为null啊？？？这个是有可能的，但是只有当page完全没有load进来的时候我们才返回0.
			else {
				localSortBuffer[(i + 1) * tuplePerPage-1] = tuple;
				return 1;
			}
		}
		return 0;
	}
	
	private Tuple mergeSort(int[] localMergePointers, TupleReader[] IOKeys, Tuple[] localSortBuffer) throws Exception {
		TupleComparator tc = new TupleComparator(this.attrList);
		int currIdx = -1; // store the index of merge pointer of the smallest element
		Tuple res = null;
		// find the first mergePointer which is not -1, store its index in currIdx
		for (int i = 0; i < localMergePointers.length; i++) {
			if (localMergePointers[i] != -1) {
				if (currIdx == -1) {
					currIdx = i;
					res = localSortBuffer[localMergePointers[currIdx]];
				} else {
					Tuple cand = localSortBuffer[localMergePointers[i]];
					if (tc.compare(cand, res) == -1) { // cand and res can be null. In this case, they are the largest!
						res = cand;
						currIdx = i;
					}
				}
			}
		}
		if (currIdx != -1) {
			localMergePointers[currIdx]++;
			if (localMergePointers[currIdx] == (currIdx + 1) * tuplePerPage) {
				readInBuffer(currIdx, IOKeys, localSortBuffer); // load a new page
				localMergePointers[currIdx] = currIdx * tuplePerPage;
				if (localSortBuffer[localMergePointers[currIdx]] == null) {
					localMergePointers[currIdx] = -1;  // check the new loaded page
				}
			}else if (localSortBuffer[localMergePointers[currIdx]] == null) {
				localMergePointers[currIdx] = -1;
			}		
		}
		return res;	
	}
	
	private String generatePath(int fileNum) {
		StringBuilder sb = new StringBuilder();
		sb.append(tempAddress);
		sb.append('/');
		for (String s: this.attrList) {
			s = s.replace('.', '_');
			sb.append(s);
			sb.append('_');
		}
		sb.append(passNum);
		sb.append("_");
		sb.append(fileNum);
		sb.append("_");
		sb.append(queryNum);
		String scratchPath = sb.toString();
		return scratchPath;
	}
	
	/**
	 * set the file address by the fileName (which contains the queryNum, fileNum and passNum)
	 * eg. temp/external-sort/exSort-scanname-1/S_A_S_B_S_C_passNum_fileNum_queryNum
	 * @param fileName
	 * @return
	 */
	private String getFileAddress(String fileName) {
		StringBuilder sb = new StringBuilder();
		sb.append(tempAddress);
		sb.append('/');
		sb.append(fileName);
		return sb.toString();
	}
	private int getFilePassNum(String fileName) {
		String[] s = fileName.split("_");
		if (s[s.length-1].equals("humanreadable")) {
			return Integer.valueOf(s[s.length-4]);
		}else return Integer.valueOf(s[s.length-3]);
		
	}
	
	@Override
	public Tuple getNextTuple() {
		try {
			return this.trs[0].readNextTuple();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void reset() {
		if (trs[0] != null) {
			this.trs[0].reset();
		}
	}
	
	@Override
	public void reset(int idx) {
		this.trs[0].resetFileChannel(idx);
	}
	
	class TupleComparator implements Comparator<Tuple>{
		private List<String> compareOrder;
		public TupleComparator (List<String> attrList) {
			this.compareOrder = attrList;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			if(compareOrder.size() == 0) return 0;
			if (o1 == null && o2 == null) {
				return 0;
			}
			if (o1 == null) {
				return 1;
			}
			if (o2 == null) {
				return -1;
			}
			for (String collumn: compareOrder) {
				int index = o1.getSchema().get(collumn);
				if (o1.getData()[index]<o2.getData()[index]) {
					return -1;
				}
				if (o1.getData()[index]>o2.getData()[index]) {
					return 1;
				}
			}
			for (int i = 0; i < o1.getSchema().size(); i++) {
				if (o1.getData()[i] < o2.getData()[i]) {
					return -1;
				} 
				if (o1.getData()[i] > o2.getData()[i]){
					return 1;
				} 
			}
			return 0;
		}
		
	}
	
	@Override
	public void printPlan(int level) {
			
		StringBuilder path = new StringBuilder();
		List<String> sortCol = this.attrList;
		
		for (int i=0; i<level; i++) {
			path.append("-");
		}
			
		path.append("ExternalSort");
		
		if ( sortCol != null) {
			path.append("[");
			
			for (int i=0; i<sortCol.size();i++) {
				path.append(sortCol.get(i));
				path.append(",");
			}
			path.deleteCharAt(path.length() -1);
			path.append("]");
		}
	
		PhysicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}
}
