package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.sql.*;

public class Connector {

  @FunctionalInterface
  public interface AfterExecuteHandler {
    void after(Connection c, boolean success);
  }

  /**
   * used for custom connection setting
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
   * if it has a value, that will be used only one time for current thread
   */
  private static ThreadLocal<CustomConnection> threadLocalCustomConnection = new ThreadLocal<>();

  private Connection conn;
  private Statement stmt;
  private AfterExecuteHandler afterExecuteHandler;

  public static void setConnectionInfo(String url, String user, String password) {
    Connector.url = url;
    Connector.user = user;
    Connector.password = password;
  }

  public static void prepareCustomConnection(Connection c, AfterExecuteHandler afterExecuteHandler) {
    threadLocalCustomConnection.set(new CustomConnection(c, afterExecuteHandler));
  }

  public static Connector prepareStatement(String sql, boolean returnGeneratedKeys) throws SQLException {
    Connection conn = null;
    PreparedStatement pst = null;
    AfterExecuteHandler afterExecuteHandler = null;

    try {
      CustomConnection customConnection = threadLocalCustomConnection.get();
      if (customConnection != null) {
        conn = customConnection.conn;
        afterExecuteHandler = customConnection.afterExecutedHandler;

        threadLocalCustomConnection.remove();
      } else {
        conn = makeDBConnection();
      }

      pst = conn.prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      Logger.i("prepareStatement: %s", sql);

      return new Connector(conn, pst, afterExecuteHandler);
    } catch (SQLException e) {
      tryClose(pst);
      tryClose(conn);
      throw e;
    }
  }

  static Connection makeDBConnection() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  private static void tryClose(AutoCloseable ac) {
    try {
      if (ac != null) {
        ac.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
  }

  private Connector(Connection conn, Statement st, AfterExecuteHandler afterExecuteHandler) {
    this.conn = conn;
    this.stmt = st;
    this.afterExecuteHandler = afterExecuteHandler;
  }

  public PreparedStatement getPreparedStatement() {
    return (this.stmt instanceof PreparedStatement ? (PreparedStatement)(this.stmt) : null);
  }

  public void executed(boolean success) {
    if (afterExecuteHandler != null) {
      afterExecuteHandler.after(conn, success);
    }
  }

  public void close() {
    tryClose(stmt);
    tryClose(conn);
  }

}

