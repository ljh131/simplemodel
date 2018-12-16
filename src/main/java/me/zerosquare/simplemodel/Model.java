package me.zerosquare.simplemodel;

import java.sql.*;
import java.util.*;
import java.time.*;
import java.util.regex.*;
import java.lang.annotation.*;
import java.lang.reflect.*;
import org.apache.commons.lang3.*;
import org.apache.commons.lang3.tuple.*;

/*
TODO

1.
@BindId
@BindColumn
@BindCreaedAt

2.
@BindColumn(name="id", id=true)
@BindColumn(name="created_at", createdAt=true)
@BindColumn(name="updated_at", updatedAt=true)

orm 사용시 execution 전에 orm을 column values로 가져오는 작업을 하는데 이 때 id, created_at 들도 가져오자. (중복이 발생하는데?)
-> 가져와서 뭘 함??

BindColumn시 name생략하면 필드명 사용?
*/

/**
# ORM 사용시 주의사항

- ~long id필드가 있어야 한다. (update/delete시)~
- bind되는 모든 field의 type은 primitive가 아니어야 한다.
- put으로 설정한 필드보다 ORM 필드가 우선한다. 즉 같은 이름의 필드가 put, ORM으로 둘 다 설정된 경우 ORM 값을 따른다.
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

  protected void beforeExecute(QueryType type) {
    data.fromAnnotation();
  }

  protected void afterExecute(QueryType queryType, boolean success) {
    if(success) {
      data.toAnnotation();
    }
  }

  /**
   * @return generatedId if exists, otherwise 0 when success, -1 when error
   */
  public long create() {
    QueryType queryType = QueryType.INSERT;
    beforeExecute(queryType);

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.INSERT);
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, 
      StringUtils.join(colnames.toArray(), ','), 
      StringUtils.join(colnames.stream().map(e -> "?").toArray(), ','));

    boolean success = false;
    Connector c = null;
    try {
      c = Connector.prepareStatement(q, true);
      PreparedStatement pst = c.getPreparedStatement();
      addParameters(pst, 0, colvals);

      pst.executeUpdate();
      ResultSet rs = pst.getGeneratedKeys();
      if (rs.next()) {
        long generatedId = rs.getLong(1);

        data.putId(generatedId);

        success = true;
        return generatedId;
      }

      return 0;
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
      afterExecute(queryType, success);
    }
    return -1;
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
    beforeExecute(QueryType.SELECT);

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

      return (List<T>)models;
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
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

  // returns affected row count
  public int update() {
    QueryType queryType = QueryType.UPDATE;
    beforeExecute(queryType);

    reserveDefaultWhereForUpdate();

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.UPDATE);
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName, 
      StringUtils.join(colnames.stream().map(c -> String.format("%s=?", c)).toArray(), ','),
      getReservedWhere());

    boolean success = false;
    Connector c = null;
    try {
      c = Connector.prepareStatement(q, true);
      PreparedStatement pst = c.getPreparedStatement();
      int last = addParameters(pst, 0, colvals);
      addParameters(pst, last, reservedWhereParams);

      success = true;
      return pst.executeUpdate();
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
      afterExecute(queryType, success);
    }
    return -1;
  }

  /**
   * update single column on db directly. (so it skips callback)
   */
  public int updateColumn(String columnName, Object value) {
    reserveDefaultWhereForUpdate();

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName, 
      String.format("%s=?", columnName),
      getReservedWhere());

    Connector c = null;
    try {
      c = Connector.prepareStatement(q, true);
      PreparedStatement pst = c.getPreparedStatement();
      int last = addParameters(pst, 0, Arrays.asList(value));
      addParameters(pst, last, reservedWhereParams);

      return pst.executeUpdate();
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
    }
    return -1;
  }

  public int delete() {
    QueryType queryType = QueryType.DELETE;
    beforeExecute(queryType);

    reserveDefaultWhereForUpdate();

    String q = String.format("DELETE FROM %s WHERE %s", tableName, 
      getReservedWhere());

    boolean success = false;
    Connector c = null;
    try {
      c = Connector.prepareStatement(q, false);
      PreparedStatement pst = c.getPreparedStatement();
      addParameters(pst, 0, reservedWhereParams);

      success = true;
      return pst.executeUpdate();
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
      afterExecute(queryType, success);
    }
    return 0;
  }

  public void setColumnValues(Map<String, Object> colvals) {
    data.setColumnValues(colvals);
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

  public Map<String, Object> getColumnValues() {
    return data.getColumnValues();
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
    if (c.isAnnotationPresent(BindTable.class)) {
      Annotation annotation = c.getAnnotation(BindTable.class);
      BindTable bc = (BindTable)annotation;
      this.tableName = bc.name();
    }
  }

  /**
   * @return 마지막으로 설정된 column index
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
