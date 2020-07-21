package me.zerosquare.simplemodel.internals;

import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Logger {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger("simplemodel");

  private static long getThreadId() {
    return Thread.currentThread().getId();
  }

  private static String makeLine(String str, Object... args) {
    return String.format(str, args);
  }

  public static void d(String str, Object... args) {
    logger.debug(makeLine(str, args));
  }

  public static void t(String str, Object... args) {
    logger.trace(makeLine(str, args));
  }

  public static void i(String str, Object... args) {
    logger.info(makeLine(str, args));
  }

  public static void w(String str, Object... args) {
    logger.warn(makeLine(str, args));
  }

  public static void e(String str, Object... args) {
    logger.error(makeLine(str, args));
  }

  public static String getExceptionString(Exception e) {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    e.printStackTrace(printWriter);
    printWriter.flush();

    return String.format("%s\n%s", e.getMessage(), writer.toString());
  }
}
