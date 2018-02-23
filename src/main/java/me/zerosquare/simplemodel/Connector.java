package me.zerosquare.simplemodel;

import java.sql.*;

public class Connector {
  public static void setConnectionInfo(String url, String user, String password) {
    Connector.url = url;
    Connector.user = user;
    Connector.password = password;
  }

  // FIXME 익셉션 발생시 close
  public static Connector executeQuery(String sql) throws SQLException {
    Connection conn = getDBConnection();
    Statement st = conn.createStatement();
    Logger.i("executeQuery: %s", sql);
    ResultSet rs = st.executeQuery(sql);

    return new Connector(conn, st, rs, 0);
  }

  // FIXME 익셉션 발생시 close
  public static Connector executeUpdate(String sql, boolean returnGeneratedKeys) throws SQLException {
    Connection conn = getDBConnection();
    Statement st = conn.createStatement();
    Logger.i("executeUpdate: %s", sql);

    int affected = st.executeUpdate(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
    if(affected == 0) {
      Logger.w("no rows affected: %s", sql);
      // XXX update시 바뀐게 없으면 affected가 나올 수 있으므로 에러는 아님
    }

    return new Connector(conn, st, returnGeneratedKeys ? st.getGeneratedKeys() : null, affected);
  }

  // FIXME 익셉션 발생시 close
  public static Connector prepareStatement(String sql, boolean returnGeneratedKeys) throws SQLException {
    Connection conn = getDBConnection();
    PreparedStatement pst = conn.prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
    Logger.i("prepareStatement: %s", sql);

    return new Connector(conn, pst, null, 0);
  }

  /*
  public static Connector beginTransaction() {
    Connection conn = getDBConnection();
    conn.setAutoCommit(false);
  }

  public void commit() {
    conn.commit();
  }
  */

  public PreparedStatement getPreparedStatement() { return (this.st instanceof PreparedStatement ? (PreparedStatement)(this.st) : null); }

  public ResultSet getResultSet() { return this.rs; }

  public int getAffactedRowCount() { return affectedRowCount; }

  public void close() {
    // TODO rs는 안닫아도 됨?
    try { if (st != null) { st.close(); } } catch(SQLException ignored) {}
    try { if (conn != null) { conn.close(); } } catch(SQLException ignored) {}
  }

  private static Connection getDBConnection() throws SQLException {
    Connection c = DriverManager.getConnection(url, user, password);
    return c;
  }

  private Connector(Connection conn, Statement st, ResultSet rs, int affectedRowCount) {
    this.conn = conn;
    this.st = st;
    this.rs = rs;
    this.affectedRowCount = affectedRowCount;
  }

  private static String url;
  private static String user;
  private static String password;

  private Connection conn;
  private Statement st;
  private ResultSet rs;
  private int affectedRowCount;

}

