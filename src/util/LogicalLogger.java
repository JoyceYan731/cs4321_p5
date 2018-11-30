package util;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
public class LogicalLogger {





		 private static Logger logger = null;
		 
		 private static void initLogger() {
			 System.setProperty("java.util.logging.SimpleFormatter.format",
		              "%5$s %n");
			 try {
				 logger = Logger.getLogger("LogicalLog");
				 FileHandler fh =  new FileHandler("LogicalQueryPlan.log");
				 logger.addHandler(fh);
				 SimpleFormatter formatter = new SimpleFormatter();
				 fh.setFormatter(formatter);
			 } catch (SecurityException e) {
				 e.printStackTrace();
			 } catch (IOException e) {
				 e.printStackTrace();
			 }
		 }
		 
		 public static Logger getLogger() {
			 if (logger == null) {
				 initLogger();
			 }
			 return logger;
		 }
	

}
