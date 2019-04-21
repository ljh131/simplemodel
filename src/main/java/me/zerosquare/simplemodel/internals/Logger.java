package me.zerosquare.simplemodel.internals;

import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Logger {
  final static org.slf4j.Logger logger = LoggerFactory.getLogger("simplemodel");

  private static long getThreadId() {
    long tid = Thread.currentThread().getId();
    return tid;
  }

  private static String makeLine(String str, Object... args) {
		String line = String.format(str, args);
    return line;
    //return String.format("(%9d) %s", getThreadId(), line);
  }
	
	public static void d(String str, Object... args){
    logger.debug(makeLine(str, args));
	}

	public static void t(String str, Object... args){
    logger.trace(makeLine(str, args));
	}

	public static void i(String str, Object... args){
    logger.info(makeLine(str, args));
	}
	
	public static void w(String str, Object... args){
    logger.warn(makeLine(str, args));
	}

	public static void e(String str, Object... args){
    logger.error(makeLine(str, args));
	}

  /*
	private static void log(String lev, String msg) {
		String line = String.format("%s [%s] %s", getTimeString(), lev, msg);
		System.out.println(line);

		writeToFile(line);
	}
	
	private static String getTimeString() {
	    Calendar calendar = new GregorianCalendar();

		String time = String.format("%04d/%02d/%02d %02d:%02d:%02d.%03d",
				calendar.get(Calendar.YEAR),
				calendar.get(Calendar.MONTH) + 1,
				calendar.get(Calendar.DATE),
				calendar.get(Calendar.HOUR),
				calendar.get(Calendar.MINUTE),
				calendar.get(Calendar.SECOND),
				calendar.get(Calendar.MILLISECOND)
				);
		
		return time;
	}
	
	private static void writeToFile(String line) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("log.txt", true));
			
			out.write(line + "\n");
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
  */

  public static void warnException(Exception e) {
    String trace = getExceptionStackTrace(e);
    w("exception has occured: %s", trace);
  }

  private static String getExceptionStackTrace(Exception e) {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    e.printStackTrace(printWriter);
    printWriter.flush();

    String stackTrace = writer.toString();
    return stackTrace;
  }
}
