package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internal.Connector;
import me.zerosquare.simplemodel.internal.Logger;
import me.zerosquare.simplemodel.internal.ModelData;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.Annotation;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Use this class directly, or extends this class to use ORM
 *
 * # Special columns
 * Special columns have predefined column names: id, created_at, updated_at
 *
 *  - id is used by find and update/delete (only if where is not specified).
 *    - If you want to use these methods, you should have id column in the table.
 *  - created_at/updated_at are also stored when row is created/updated.
 *    - If you want to store them, you should put created_at/updated_at with null value or map in the ORM
 */
public class Model {

  public static Model table(String tableName) {
    return new Model(tableName);
  }

  public Model(String tableName) {
    this.tableName = tableName;
  }

  public Model() {
    trySetTableNameFromAnnotation();
  }

  public enum QueryType {
    INSERT,
    SELECT,
    UPDATE,
    DELETE
  }

  /**
   * @return generatedId if exists, otherwise 0 when success, -1 when error
   */
  public long create() throws SQLException {
    QueryType queryType = QueryType.INSERT;
    _beforeExecute(queryType);

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.INSERT);
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, 
      StringUtils.join(colnames.toArray(), ','), 
      StringUtils.join(colnames.stream().map(e -> "?").toArray(), ','));

    return execute(queryType, q, pst -> {
      addParameters(pst, 0, colvals);

      pst.executeUpdate();
      ResultSet rs = pst.getGeneratedKeys();
      if (rs.next()) {
        long generatedId = rs.getLong(1);

        data.putId(generatedId);

        return generatedId;
      }

      return -1;
    });
  }

  public <T extends Model> T select(String selectClause, Object... args) {
		String c = String.format(selectClause, args);
    reservedSelect = c;
    return (T)this;
  }

  private static final Pattern findJoinPattern = Pattern.compile("\\bjoin\\b", Pattern.CASE_INSENSITIVE);

  public <T extends Model> T joins(String joinClause, Object... args) {
    if(!findJoinPattern.matcher(joinClause).find()) {
      joinClause = "JOIN " + joinClause;
    }

		String c = String.format(joinClause, args);
    reservedJoin += " " + c;
    return (T)this;
  }

  public <T extends Model> T order(String orderClause, Object... args) {
		String c = String.format(orderClause, args);
    reservedOrderby = c;
    return (T)this;
  }

  public <T extends Model> T limit(long limitNumber) {
		String c = String.format("%d", limitNumber);
    reservedLimit = c;
    return (T)this;
  }

  public <T extends Model> T offset(long offsetNumber) {
		String c = String.format("%d", offsetNumber);
    reservedOffset = c;
    return (T)this;
  }

  public <T extends Model> T resetWhere() {
    reservedWhere = "";
    reservedWhereParams = new ArrayList<>();
    return (T)this;
  }

  public <T extends Model> T where(String whereClause, Object... args) {
    if(StringUtils.isBlank(whereClause)) return (T)this;

    reservedWhereParams.addAll(Arrays.asList(args));

    if(reservedWhere.isEmpty()) {
      reservedWhere = whereClause;
    } else {
      reservedWhere += " and " + whereClause;
    }
    return (T)this;
  }

  // returns empty list if no result found
  public <T extends Model> List<T> fetch() {
    QueryType queryType = QueryType.SELECT;
    _beforeExecute(queryType);

    String q = String.format("SELECT %s from %s", 
        reservedSelect.isEmpty() ? "*" : reservedSelect, 
        tableName);
    if(!reservedJoin.isEmpty()) {
      q += String.format(" %s", reservedJoin);
    }
    if(!reservedWhere.isEmpty()) {
      q += String.format(" WHERE %s", reservedWhere);
    }
    if(!reservedOrderby.isEmpty()) {
      q += String.format(" ORDER BY %s", reservedOrderby);
    }
    if(!reservedLimit.isEmpty()) {
      q += String.format(" LIMIT %s", reservedLimit);
    }
    if(!reservedOffset.isEmpty()) {
      q += String.format(" OFFSET %s", reservedOffset);
    }

    boolean success = false;
    Connector c = null;
    try {
      c = Connector.prepareStatement(q, false);
      PreparedStatement pst = c.getPreparedStatement();
      addParameters(pst, 0, reservedWhereParams);
      ResultSet rs = pst.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();

      ArrayList<Model> models = new ArrayList<>();

      while(rs.next()) {
        Map<String, Object> colvals = new HashMap<>();

        for(int col = 1; col <= cols; col++) {
          String table = meta.getTableName(col);
          String key = meta.getColumnName(col);
          int type = meta.getColumnType(col);
          Object val;

          // TODO need more
          switch(type) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.BOOLEAN:
              val = rs.getBoolean(col);
              break;

            case Types.SMALLINT:
            case Types.INTEGER:
              val = rs.getInt(col);
              break;

            case Types.BIGINT:
              val = rs.getLong(col);
              break;

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
              val = rs.getString(col);
              break;
              
            case Types.TIMESTAMP:
              val = rs.getTimestamp(col);
              break;

            default:
              throw new RuntimeException(String.format("Unknown column type! %s(%d) %s on column %s", 
                    meta.getColumnTypeName(col), type, meta.getColumnClassName(col), key));
          }

          Logger.t("fetched - table: %s key: %s type: %s val: %s", 
              table, key, type, val == null ? "(null)" : val.toString());

          if(!table.equals(tableName) && table.length() > 0) {
            key = String.format("%s.%s", table, key);
          }
          colvals.put(key, val);
        }

        Model model = newInstance();
        model.tableName = tableName;
        model.setColumnValues(colvals);
        model.data.toAnnotation();
        models.add(model);
      }

      success = true;
      return (List<T>)models;
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
      _afterExecute(queryType, success);
    }

    return null;
  }

  /**
   * This method is helpful for these kind of queries - `select count(id) ...`
   * Note that it does not use `limit 1`.
   */
  public <T extends Model> T fetchFirst() {
    return (T)fetch().get(0);
  }

  // returns null if no result
  public <T extends Model> T findBy(String whereClause, Object... args) {
    List<Model> r = where(whereClause, args).limit(1).fetch();
    if(r == null || r.size() == 0) return null;
    return (T)r.get(0);
  }

  // returns null if no result
  public <T extends Model> T find(long id) {
    return findBy(makeWhereWithFindId(id));
  }

  /**
   * @return affected row count, -1 when error
   */
  public long update() throws SQLException {
    QueryType queryType = QueryType.UPDATE;
    _beforeExecute(queryType);

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.UPDATE);
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName, 
      StringUtils.join(colnames.stream().map(c -> String.format("%s=?", c)).toArray(), ','),
      getReservedWhere());

    return execute(queryType, q, pst -> {
      int last = addParameters(pst, 0, colvals);
      addParameters(pst, last, reservedWhereParams);

      return pst.executeUpdate();
    });
  }

  /**
   * update single column on db directly. (so it skips callback)
   *
   * @return affected row count, -1 when error
   */
  public long updateColumn(String columnName, Object value) throws SQLException {
    reserveDefaultWhereForUpdate();

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName, 
      String.format("%s=?", columnName),
      getReservedWhere());

    return execute(null, q, pst -> {
      int last = addParameters(pst, 0, Arrays.asList(value));
      addParameters(pst, last, reservedWhereParams);

      return pst.executeUpdate();
    });
  }

  /**
   * @return affected row count, -1 when error
   */
  public long delete() throws SQLException {
    QueryType queryType = QueryType.DELETE;
    _beforeExecute(queryType);

    String q = String.format("DELETE FROM %s WHERE %s", tableName,
      getReservedWhere());

    return execute(queryType, q, pst -> {
      addParameters(pst, 0, reservedWhereParams);

      return pst.executeUpdate();
    });
  }

  /**
   * put column and value for create/update
   */
  public <T extends Model> T put(String key, Object val) {
    data.put(key, val);
    return (T)this;
  }

  /**
   * get column value
   */
  public Object get(String columnName) {
    return data.get(columnName);
  }

  public String getString(String columnName) {
    return (String)get(columnName);
  }

  public int getInt(String columnName) {
    return (int)get(columnName);
  }

  public long getLong(String columnName) {
    return (long)get(columnName);
  }

  public Long getId() {
    return data.getId();
  }

  public String getTableName() {
    return tableName;
  }

  public String dump() {
    String ds = "";
    ds += String.format("tableName: %s\n", tableName);
    ds += String.format("columnValues:\n", data.dump());
    return ds;
  }

  protected void beforeExecute(QueryType type) {}
  protected void afterExecute(QueryType type, boolean success) {}

  private void _beforeExecute(QueryType queryType) {
    beforeExecute(queryType);
    data.fromAnnotation();

    if(queryType == QueryType.UPDATE || queryType == QueryType.DELETE) {
      reserveDefaultWhereForUpdate();
    }
  }

  private void _afterExecute(QueryType queryType, boolean success) {
    if(success) {
      data.toAnnotation();
    }
    afterExecute(queryType, success);
  }

  private void setColumnValues(Map<String, Object> colvals) {
    data.setColumnValues(colvals);
  }

  private Map<String, Object> getColumnValues() {
    return data.getColumnValues();
  }

  private Model newInstance() {
    try {
      return getClass().newInstance();
    } catch(InstantiationException e) {
      Logger.warnException(e);
    } catch(IllegalAccessException e) {
      Logger.warnException(e);
    }
    return null;
  }

  private void trySetTableNameFromAnnotation() {
    Class c = this.getClass();
    if (c.isAnnotationPresent(Table.class)) {
      Annotation annotation = c.getAnnotation(Table.class);
      Table bc = (Table)annotation;
      this.tableName = bc.name();
    }
  }

  /**
   * @return last set column index
   */
  private int addParameters(PreparedStatement pst, int lastColumnIndex, List<Object> vals) throws SQLException {
    int colidx = 0;

    for(int i = 0; i < vals.size(); i++) {
      Object val = vals.get(i);
      colidx = lastColumnIndex + 1 + i;

      if(val == null) {
        Logger.t("preparams - idx: %d colidx: %d val: (null)", i, colidx);
        continue;
      }

      Logger.t("preparams - idx: %d colidx: %d val: %s", i, colidx, val.toString());

      // TODO need more
      if(val instanceof Integer) {
        pst.setInt(colidx, (Integer)val);
      } else if(val instanceof Long) {
        pst.setLong(colidx, (Long)val);
      } else if(val instanceof Boolean) {
        pst.setBoolean(colidx, (Boolean)val);
      } else if(val instanceof String) {
        pst.setString(colidx, (String)val);
      } else if(val instanceof Timestamp) {
        pst.setTimestamp(colidx, (Timestamp)val);
      } else if(val instanceof java.sql.Date) {
        pst.setDate(colidx, (java.sql.Date)val);
      } else if(val instanceof LocalDate) {
        pst.setDate(colidx, java.sql.Date.valueOf((LocalDate)val));
      } else {
        Logger.w("preparams - unrecognize type for val: %s", val.toString());
      }
    }
    return colidx;
  }

  private void reserveDefaultWhereForUpdate() {
    if(!StringUtils.isBlank(reservedWhere)) return;
    reservedWhere = makeDefaultWhereForUpdate();
    Logger.t("default where for update reserved: %s", reservedWhere);
  }

  // update/delete시 조건문을 지정하지 않을 경우 사용하는 where절
  private String makeDefaultWhereForUpdate() {
    Long id = data.getId();
    String defaultWhere = makeWhereWithFindId(id);
    return defaultWhere;
  }

  private String makeWhereWithFindId(long id) {
    String where = StringUtils.isBlank(reservedJoin) ? 
      String.format("id=%d", id) :
      String.format("%s.id=%d", tableName, id);
    return where;
  }

  private String getReservedWhere() {
    if(StringUtils.isBlank(reservedWhere)) {
      throw new IllegalArgumentException("no where clause specified!");
    }
    return reservedWhere;
  }

  @FunctionalInterface
  private interface ExecuteFunction {
    long call(PreparedStatement pst) throws SQLException;
  }

  private long execute(QueryType queryType, String sql, ExecuteFunction functor) throws SQLException {
    long result = -1;
    Connector c = null;
    try {
      c = Connector.prepareStatement(sql, true);
      PreparedStatement pst = c.getPreparedStatement();

      result = functor.call(pst);
    } catch(SQLException e) {
      Logger.warnException(e);
      throw e;
    } finally {
      if(c != null) { c.close(); }
      if(queryType != null) {
        _afterExecute(queryType, result >= 0);
      }
    }
    return result;
  }

  private String tableName;
  private ModelData data = new ModelData(this);

  private String reservedWhere = "";
  private ArrayList<Object> reservedWhereParams = new ArrayList<>();

  private String reservedSelect = "";
  private String reservedJoin = "";
  private String reservedOrderby = "";
  private String reservedLimit = "";
  private String reservedOffset = "";

}
