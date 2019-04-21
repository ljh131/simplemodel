package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internals.Logger;

import java.sql.*;

public class Connector {
  public static void setConnectionInfo(String url, String user, String password) {
    Connector.url = url;
    Connector.user = user;
    Connector.password = password;
  }

  public static Connector prepareStatement(String sql, boolean returnGeneratedKeys) throws SQLException {
    Connection conn = null;
    PreparedStatement pst = null;

    try {
      conn = getDBConnection();
      pst = conn.prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      Logger.i("prepareStatement: %s", sql);

      return new Connector(conn, pst);
    } catch (SQLException e) {
      tryClose(pst);
      tryClose(conn);
      throw e;
    }
  }

  public PreparedStatement getPreparedStatement() {
    return (this.st instanceof PreparedStatement ? (PreparedStatement)(this.st) : null);
  }

  public void close() {
    tryClose(st);
    tryClose(conn);
  }

  private static Connection getDBConnection() throws SQLException {
    Connection c = DriverManager.getConnection(url, user, password);
    return c;
  }

  private static void tryClose(AutoCloseable ac) {
    try {
      if (ac != null) {
        ac.close();
      }
    } catch (Exception ignored) {
    }
  }

  private Connector(Connection conn, Statement st) {
    this.conn = conn;
    this.st = st;
  }

  private static String url;
  private static String user;
  private static String password;

  private Connection conn;
  private Statement st;

}

