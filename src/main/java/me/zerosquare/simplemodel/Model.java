package me.zerosquare.simplemodel;

import java.util.*;
import java.sql.*;
import java.lang.annotation.*;
import java.lang.reflect.*;
import org.apache.commons.lang3.*;
import org.apache.commons.lang3.tuple.*;

/**
* ORM 사용시 주의사항
long id필드가 있어야 한다. (update/delete시)
bind되는 모든 field의 type은 primitive가 아니어야 한다.
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

  // callback
  protected void beforeExecute(QueryType type) {}
  //protected abstract void afterExecute(QueryType type);

  /**
   * @return generatedId if exists, otherwise 0 when success, -1 when error
   */
  public long create() {
    beforeExecute(QueryType.INSERT);

    fillObjectsFromAnnotation();

    Pair<ArrayList<String>, ArrayList<Object>> nvs = getColumnNameAndValues();
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, 
      StringUtils.join(colnames.toArray(), ','), 
      StringUtils.join(colnames.stream().map(e -> "?").toArray(), ','));

    Connector c = null;
    try {
      c = Connector.prepareStatement(q, true);
      PreparedStatement pst = c.getPreparedStatement();
      prepareParameters(pst, 0, colvals);

      pst.executeUpdate();
      ResultSet rs = pst.getGeneratedKeys();
      if (rs.next()) {
        long generatedId = rs.getLong(1);

        // id를 obj와 annotation에 채우자
        put("id", generatedId);
        fillSingleObjectToAnnotation("id", generatedId);

        return generatedId;
      }
      return 0;
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
    }
    return -1;
  }

  public int update() {
    return update(makeDefaultWhereForUpdate());
  }

  // returns affected row count
  public int update(String whereClause, Object... args) {
    beforeExecute(QueryType.UPDATE);

    fillObjectsFromAnnotation();

    Pair<ArrayList<String>, ArrayList<Object>> nvs = getColumnNameAndValues();
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName, 
      StringUtils.join(colnames.stream().map(c -> String.format("%s=?", c)).toArray(), ','),
      whereClause);

    Connector c = null;
    try {
      c = Connector.prepareStatement(q, true);
      PreparedStatement pst = c.getPreparedStatement();
      int last = prepareParameters(pst, 0, colvals);
      prepareParameters(pst, last, Arrays.asList(args));

      return pst.executeUpdate();
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
    }
    return -1;
  }

  public Model where(String whereClause, Object... args) {
    reservedWhereParams.addAll(Arrays.asList(args));

    if(reservedWhere.isEmpty()) {
      reservedWhere = whereClause;
    } else {
      reservedWhere += " and " + whereClause;
    }
    return this;
  }

  public Model order(String orderClause, Object... args) {
		String c = String.format(orderClause, args);
    reservedOrderby = c;
    return this;
  }

  public Model limit(long limitNumber) {
		String c = String.format("%d", limitNumber);
    reservedLimit = c;
    return this;
  }

  public Model select(String selectClause, Object... args) {
		String c = String.format(selectClause, args);
    reservedSelect = c;
    return this;
  }

  // returns empty list if no result found
  public <T extends Model> List<T> fetch() {
    beforeExecute(QueryType.SELECT);

    String q = String.format("SELECT %s from %s", 
        reservedSelect.isEmpty() ? "*" : reservedSelect, 
        tableName);
    if(!reservedWhere.isEmpty()) {
      q += String.format(" WHERE %s", reservedWhere);
    }
    if(!reservedOrderby.isEmpty()) {
      q += String.format(" ORDER BY %s", reservedOrderby);
    }
    if(!reservedLimit.isEmpty()) {
      q += String.format(" LIMIT %s", reservedLimit);
    }

    Connector c = null;
    try {
      c = Connector.prepareStatement(q, false);
      PreparedStatement pst = c.getPreparedStatement();
      prepareParameters(pst, 0, reservedWhereParams);
      ResultSet rs = pst.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();

      ArrayList<Model> models = new ArrayList<>();

      while(rs.next()) {
        Map<String, Object> m = new HashMap<>();

        for(int col = 1; col <= cols; col++) {
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

          Logger.t("key %s type %s val %s", key, type, val == null ? "(null)" : val.toString());

          m.put(key, val);
        }

        Model model = newInstance();
        model.objects = m;
        model.fillObjectsToAnnotation();
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

  // returns null if no result
  public <T extends Model> T findBy(String whereClause, Object... args) {
    List<Model> r = where(whereClause, args).limit(1).fetch();
    if(r == null || r.size() == 0) return null;
    return (T)r.get(0);
  }

  // returns null if no result
  public <T extends Model> T find(long id) {
    return findBy(String.format("id=%d", id));
  }

  public int delete() {
    return delete(makeDefaultWhereForUpdate());
  }

  public int delete(String whereClause, Object... args) {
    beforeExecute(QueryType.DELETE);

    String q = String.format("DELETE FROM %s WHERE %s", tableName, 
      whereClause);

    Connector c = null;
    try {
      c = Connector.prepareStatement(q, false);
      PreparedStatement pst = c.getPreparedStatement();
      prepareParameters(pst, 0, Arrays.asList(args));
      return pst.executeUpdate();
    } catch(SQLException e) {
      Logger.warnException(e);
    } finally {
      if(c != null) { c.close(); }
    }
    return 0;
  }

  /** 
   * put column and value for create/update
   */
  public void put(String key, Object val) {
    objects.put(key, val);
  }

  /**
   * get column value
   */
  public Object get(String columnName) {
    return objects.get(columnName);
  }

  /**
   * get column value named `id`
   */
  public Long getId() {
    Object o = get("id");
    return o != null ? Long.parseLong(o.toString()) : null;
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

  private Pair<ArrayList<String>, ArrayList<Object>> getColumnNameAndValues() {
    ArrayList<String> colnames = new ArrayList<>();
    ArrayList<Object> colvals = new ArrayList<>();

    for (Map.Entry<String, Object> e : objects.entrySet()) {
      String key = e.getKey();
      Object val = e.getValue();
      if(!isValidKeyValue(key, val)) continue;

      colnames.add(key);
      colvals.add(val);
    }

    return new MutablePair<>(colnames, colvals);
  }

  // 유효하지 않은 k/v가 insert/update되지 않도록 스킵
  private boolean isValidKeyValue(String key, Object val) {
    if(key.equals("id") || key.equals("created_at")) {
      return false;
    }

    if(val == null) {
      //Logger.w("val is null! key '%s' is ignored for insert/update", key.toString());
      return false;
    }

    return true;
  }

  /**
   * @return 마지막으로 설정된 column index
   */
  private int prepareParameters(PreparedStatement pst, int lastColumnIndex, List<Object> vals) throws SQLException {
    int colidx = 0;

    for(int i = 0; i < vals.size(); i++) {
      Object val = vals.get(i);
      colidx = lastColumnIndex + 1 + i;

      if(val == null) {
        Logger.t("idx %d colidx %d val null", i, colidx);
        continue;
      }

      Logger.t("idx %d colidx %d val %s", i, colidx, val.toString());

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
      } else {
        Logger.w("unrecognize type for val: %s", val.toString());
      }
    }
    return colidx;
  }

  // update/delete시 조건문을 지정하지 않을 경우 사용하는 where절
  private String makeDefaultWhereForUpdate() {
    Long id = getId();
    if(id == null) {
      id = getIdFromAnnotation();
      if(id == null) {
        // TODO throws new Exception();
      }
    }
    String defaultWhere = String.format("id=%d", id);
    return defaultWhere;
  }

  private Long getIdFromAnnotation() {
    Object o = this;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        BindColumn bc = field.getAnnotation(BindColumn.class);
        if(bc.name().equals("id")) {
          try {
            return (Long)field.get(o);
          } catch(IllegalAccessException e) {
            Logger.warnException(e);
          }
        }
      }
    }
    return null;
  }

  private void fillObjectsFromAnnotation() {
    Object o = this;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        checkAnnotationField(field);

        BindColumn bc = field.getAnnotation(BindColumn.class);

        try {
          Object val = field.get(o);
          Logger.t("from annotation - %s : %s", bc.name(), val);
          put(bc.name(), val);
        } catch (IllegalAccessException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  private void fillObjectsToAnnotation() {
    Object o = this;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        checkAnnotationField(field);

        BindColumn bc = field.getAnnotation(BindColumn.class);

        try {
          Object val = get(bc.name());
          Logger.t("to annotation - %s : %s", bc.name(), val);
          SetFieldValue(o, field, val);
        } catch (IllegalArgumentException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  private void fillSingleObjectToAnnotation(String name, Object val) {
    Object o = this;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        BindColumn bc = field.getAnnotation(BindColumn.class);

        if(bc.name().equals(name)) {
          Logger.t("to annotation - %s : %s", name, val);
          SetFieldValue(o, field, val);
        }
      }
    }
  }

  private void SetFieldValue(Object o, Field field, Object val) {
    // Integer를 Long에 넣을 수 있도록 한다.
    try {
      // TODO need more
      if(val != null && field.getType() == Long.class && val instanceof Integer) {
        field.set(o, Long.valueOf((Integer)val));
      } else {
        field.set(o, val);
      }
    } catch (IllegalAccessException e) {
      // ignore me
      Logger.warnException(e);
    }
  }

  private void checkAnnotationField(Field field) {
    if(field.getType().isPrimitive()) {
      throw new RuntimeException(String.format("field '%s %s' should not be primitive!", field.getType().getName(), field.getName()));
    }
  }

  private String tableName;

  private Map<String, Object> objects = new HashMap<>();

  private String reservedWhere = "";
  private ArrayList<Object> reservedWhereParams = new ArrayList<>();
  private String reservedOrderby = "";
  private String reservedLimit = "";
  private String reservedSelect = "";

}
