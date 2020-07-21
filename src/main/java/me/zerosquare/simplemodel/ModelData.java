package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.Model.QueryType;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.internals.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class ModelData {

  /**
   * saved after any successful query execution (select, insert, update, delete)
   */
  private Map<String, Object> oldColumnValues = new HashMap<>();

  /**
   * hold column name and column values for any operations
   */
  private Map<String, Object> columnValues = new HashMap<>();


  /*
   * These are predefined columns.
   */

  private static final String COLUMN_NAME_ID = "id";
  private static final String COLUMN_NAME_CREATED_AT = "created_at";
  private static final String COLUMN_NAME_UPDATED_AT = "updated_at";

  void setColumnValues(Map<String, Object> colvals) {
    columnValues = colvals;
  }

  /**
   * save column values at this moment so track the modified columns for update modified only
   */
  void saveColumnValues() {
    oldColumnValues = new HashMap<>(columnValues);
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

  /**
   * build column name list and value list according to column values.
   *
   * @param queryType
   * @return
   */
  Pair<ArrayList<String>, ArrayList<Object>> buildColumnNameAndValues(Model.QueryType queryType, boolean modifiedColumnsOnly) {
    ArrayList<String> colnames = new ArrayList<>();
    ArrayList<Object> colvals = new ArrayList<>();

    Map<String, Object> cvs;

    if (modifiedColumnsOnly) {
      cvs = getModifiedColumnValues();
    } else {
      cvs = columnValues;
    }

    for (Map.Entry<String, Object> e : cvs.entrySet()) {
      String key = e.getKey();

      Object val = e.getValue();

      if (!isValidKeyValue(key, val)) continue;

      colnames.add(key);
      colvals.add(val);
    }

    // add created at and updated at
    if (queryType == QueryType.INSERT && containsKey(COLUMN_NAME_CREATED_AT)) {
      colnames.add(COLUMN_NAME_CREATED_AT);
      colvals.add(get(COLUMN_NAME_CREATED_AT, new Timestamp(System.currentTimeMillis())));
    } else if (queryType == QueryType.UPDATE && containsKey(COLUMN_NAME_UPDATED_AT)) {
      colnames.add(COLUMN_NAME_UPDATED_AT);
      colvals.add(get(COLUMN_NAME_UPDATED_AT, new Timestamp(System.currentTimeMillis())));
    }

    return new MutablePair<>(colnames, colvals);
  }

  Map<String, Object> getModifiedColumnValues() {
      Map<String, Object> modified = new HashMap<>();

      for (Map.Entry<String, Object> kv : columnValues.entrySet()) {
        String k = kv.getKey();
        Optional<Object> v = Optional.ofNullable(kv.getValue());
        if (oldColumnValues.containsKey(k) && !Optional.ofNullable(oldColumnValues.get(k)).equals(v)) {
          modified.put(k, v.orElse(null));
        }
      }

      return modified;
  }

  Map<String, Object> getColumnValuesFromResultSet(String tableName, ResultSet rs) throws SQLException {
    Map<String, Object> colvals = new HashMap<>();

    ResultSetMetaData meta = rs.getMetaData();
    int cols = meta.getColumnCount();

    for(int col = 1; col <= cols; col++) {
      // transform into lower case because h2 db returns CAPITALIZED table/column names!
      String table = meta.getTableName(col).toLowerCase();
      String key = meta.getColumnName(col).toLowerCase();
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

      if (!table.equals(tableName) && table.length() > 0) {
        key = String.format("%s.%s", table, key);
      }
      colvals.put(key, val);
    }

    return colvals;
  }

  /**
   * to skip invalid columns when insert/update
   */
  private boolean isValidKeyValue(String key, Object val) {
    if (key.equals(COLUMN_NAME_ID) ||
      key.equals(COLUMN_NAME_CREATED_AT) || 
      key.equals(COLUMN_NAME_UPDATED_AT)) {
      return false;
    }

    // FIXME cannot update to null due to this!
    return val != null;
  }

  void columnValuesFromAnnotation(Object o) {
    for (Class c = o.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
      Field[] fields = c.getDeclaredFields();
      for (Field field : fields) {
        if (field.isAnnotationPresent(Column.class)) {
          validateAnnotatedField(field);

          Column bc = field.getAnnotation(Column.class);
          String name = columnFieldName(bc, field);

          try {
            Object val = field.get(o);
            Logger.t("from annotation - %s : %s", name, val);
            put(name, val);
          } catch (IllegalAccessException e) {
            // ignore me
            Logger.w(Logger.getExceptionString(e));
          }
        }
      }
    }
  }

  void columnValuesToAnnotation(Object o) {
    for (Class c = o.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
      Field[] fields = c.getDeclaredFields();
      for (Field field : fields) {
        if (field.isAnnotationPresent(Column.class)) {
          validateAnnotatedField(field);

          Column bc = field.getAnnotation(Column.class);
          String name = columnFieldName(bc, field);

          try {
            Object val = get(name);
            Logger.t("to annotation - %s : %s", name, val);

            setFieldValue(o, field, val);
          } catch (IllegalArgumentException e) {
            // ignore me
            Logger.w(Logger.getExceptionString(e));
          }
        }
      }
    }
  }

  private String columnFieldName(Column col, Field field) {
    String name = col.name();
    if (StringUtils.isBlank(name)) name = field.getName();
    return name.toLowerCase();
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
      if (val != null && field.getType() == Long.class && val instanceof Integer) {
        field.set(o, Long.valueOf((Integer)val));
      } else {
        field.set(o, val);
      }
    } catch (IllegalAccessException e) {
      // ignore me
      Logger.w("fail to setFieldValue - %s", Logger.getExceptionString(e));
    }
  }

  private void validateAnnotatedField(Field field) {
    if (field.getType().isPrimitive()) {
      throw new RuntimeException(String.format("field '%s %s' should not be primitive!", field.getType().getName(), field.getName()));
    }
  }

}