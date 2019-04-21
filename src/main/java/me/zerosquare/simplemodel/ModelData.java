package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.Model.QueryType;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.internal.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ModelData {

  void setColumnValues(Map<String, Object> colvals) {
    columnValues = colvals;
  }

  Map<String, Object> getColumnValues() {
    return columnValues;
  }

  void put(String key, Object val) {
    columnValues.put(key, val);
  }

  Object get(String key) {
    return columnValues.get(key);
  }

  Object get(String columnName, Object fallback) {
    Object v = get(columnName);
    return v != null ? v : fallback;
  }

  boolean containsKey(String key) {
    return columnValues.containsKey(key);
  }

  void putId(Long id) {
    put(COLUMN_NAME_ID, id);
  }

  Long getId() {
    Object o = get(COLUMN_NAME_ID);
    return o != null ? Long.parseLong(o.toString()) : null;
  }

  Pair<ArrayList<String>, ArrayList<Object>> buildColumnNameAndValues(Model.QueryType queryType) {
    ArrayList<String> colnames = new ArrayList<>();
    ArrayList<Object> colvals = new ArrayList<>();

    for (Map.Entry<String, Object> e : columnValues.entrySet()) {
      String key = e.getKey();
      Object val = e.getValue();
      if(!isValidKeyValue(key, val)) continue;

      colnames.add(key);
      colvals.add(val);
    }

    // add created at and updated at
    if(queryType == QueryType.INSERT && containsKey(COLUMN_NAME_CREATED_AT)) {
      colnames.add(COLUMN_NAME_CREATED_AT);
      colvals.add(get(COLUMN_NAME_CREATED_AT, new Timestamp(System.currentTimeMillis())));
    } else if(queryType == QueryType.UPDATE && containsKey(COLUMN_NAME_UPDATED_AT)) {
      colnames.add(COLUMN_NAME_UPDATED_AT);
      colvals.add(get(COLUMN_NAME_UPDATED_AT, new Timestamp(System.currentTimeMillis())));
    }

    return new MutablePair<>(colnames, colvals);
  }

  Map<String, Object> getFromResultSet(String tableName, ResultSet rs) throws SQLException {
    Map<String, Object> colvals = new HashMap<>();

    ResultSetMetaData meta = rs.getMetaData();
    int cols = meta.getColumnCount();

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

    return colvals;
  }

  String dump() {
    String ds = "";
    for (Map.Entry<String, Object> entry : columnValues.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      ds += String.format(" %s : %s\n", key, value);
    }
    return ds;
  }

  /**
   * to skip invalid columns when insert/update
   */
  private boolean isValidKeyValue(String key, Object val) {
    if(key.equals(COLUMN_NAME_ID) ||
      key.equals(COLUMN_NAME_CREATED_AT) || 
      key.equals(COLUMN_NAME_UPDATED_AT)) {
      return false;
    }

    if(val == null) {
      //Logger.w("val is null! key '%s' is ignored for insert/update", key.toString());
      return false;
    }

    return true;
  }

  void fromAnnotation(Object o) {
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(Column.class)) {
        assertAnnotatedField(field);

        Column bc = field.getAnnotation(Column.class);
        String name = columnFieldName(bc, field);

        try {
          Object val = field.get(o);
          Logger.t("from annotation - %s : %s", name, val);
          put(name, val);
        } catch (IllegalAccessException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  void toAnnotation(Object o) {
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(Column.class)) {
        assertAnnotatedField(field);

        Column bc = field.getAnnotation(Column.class);
        String name = columnFieldName(bc, field);

        try {
          Object val = get(name);
          Logger.t("to annotation - %s : %s", name, val);
          setFieldValue(o, field, val);
        } catch (IllegalArgumentException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  private String columnFieldName(Column col, Field field) {
    String name = col.name();
    if(StringUtils.isBlank(name)) name = field.getName();
    return name;
  }

  /**
   * let Integer be put in Long
   * @param o
   * @param field
   * @param val
   */
  private void setFieldValue(Object o, Field field, Object val) {
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

  private void assertAnnotatedField(Field field) {
    if(field.getType().isPrimitive()) {
      throw new RuntimeException(String.format("field '%s %s' should not be primitive!", field.getType().getName(), field.getName()));
    }
  }

  private Map<String, Object> columnValues = new HashMap<>();

  private static final String COLUMN_NAME_ID = "id";
  private static final String COLUMN_NAME_CREATED_AT = "created_at";
  private static final String COLUMN_NAME_UPDATED_AT = "updated_at";

}