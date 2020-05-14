package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.sql.*;

public class Connector {

  /**
   * Called after SQL statement executed. You can close connection or cleanup.
   */
  @FunctionalInterface
  public interface AfterExecuteHandler {
    void after(Connection c, boolean success);
  }

  /**
   * Used for custom connection setting
   */
  private static class CustomConnection {
    private Connection conn;
    private AfterExecuteHandler afterExecutedHandler;

    public CustomConnection(Connection c, AfterExecuteHandler afterExecuteHandler) {
      this.conn = c;
      this.afterExecutedHandler = afterExecuteHandler;
    }
  }

  private static String url;
  private static String user;
  private static String password;

  /**
   * If it has a value, that will be used only one time for current thread
   */
  private static ThreadLocal<CustomConnection> threadLocalCustomConnection = new ThreadLocal<>();

  private Connection conn;
  private Statement stmt;

  private boolean isCustomConnection;
  private AfterExecuteHandler afterExecuteHandler;

  public static void setConnectionInfo(String url, String user, String password) {
    Connector.url = url;
    Connector.user = user;
    Connector.password = password;
  }

  /**
   * Use specified connection for this thread until disable called
   */
  public static void enableCustomConnection(Connection c, AfterExecuteHandler afterExecuteHandler) {
    threadLocalCustomConnection.set(new CustomConnection(c, afterExecuteHandler));
  }

  public static void disableCustomConnection() {
    threadLocalCustomConnection.remove();
  }

  public static Connector prepareStatement(String sql, boolean returnGeneratedKeys) throws SQLException {
    Connection conn = null;
    PreparedStatement pst = null;
    AfterExecuteHandler afterExecuteHandler = null;
    boolean isCustomConnection = false;

    try {
      CustomConnection customConnection = threadLocalCustomConnection.get();
      if (customConnection != null) {
        conn = customConnection.conn;
        afterExecuteHandler = customConnection.afterExecutedHandler;
        isCustomConnection = true;
      } else {
        conn = makeDBConnection();
      }

      pst = conn.prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      Logger.i("prepareStatement: %s", sql);

      return new Connector(conn, pst, isCustomConnection, afterExecuteHandler);
    } catch (SQLException e) {
      tryClose(pst);
      tryClose(conn);
      throw e;
    }
  }

  static Connection makeDBConnection() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  public static void tryClose(AutoCloseable ac) {
    try {
      if (ac != null) {
        ac.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
  }

  private Connector(Connection conn, Statement st, boolean isCustomConnection, AfterExecuteHandler afterExecuteHandler) {
    this.conn = conn;
    this.stmt = st;
    this.isCustomConnection = isCustomConnection;
    this.afterExecuteHandler = afterExecuteHandler;
  }

  public PreparedStatement getPreparedStatement() {
    return (this.stmt instanceof PreparedStatement ? (PreparedStatement)(this.stmt) : null);
  }

  public void executed(boolean success) {
    if (isCustomConnection) {
      if (afterExecuteHandler != null) {
        afterExecuteHandler.after(conn, success);
      }
    } else {
      close();
    }
  }

  public void close() {
    tryClose(stmt);
    tryClose(conn);
  }

}
