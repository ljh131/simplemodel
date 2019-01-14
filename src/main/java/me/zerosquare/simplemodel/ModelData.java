package me.zerosquare.simplemodel;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import me.zerosquare.simplemodel.Model.QueryType;

public class ModelData {

  public ModelData(Model m) {
    this.model = m;
  }

  public void setColumnValues(Map<String, Object> colvals) {
    columnValues = colvals;
  }

  public Map<String, Object> getColumnValues() {
    return columnValues;
  }

  public void put(String key, Object val) {
    columnValues.put(key, val);
  }

  public Object get(String key) {
    return columnValues.get(key);
  }

  public Object get(String columnName, Object fallback) {
    return containsKey(columnName) ? get(columnName) : fallback;
  }

  public boolean containsKey(String key) {
    return columnValues.containsKey(key);
  }

  public void putId(Long id) {
    put(COLUMN_NAME_ID, id);
  }

  public Long getId() {
    Object o = get(COLUMN_NAME_ID);
    return o != null ? Long.parseLong(o.toString()) : null;
  }

  public Pair<ArrayList<String>, ArrayList<Object>> buildColumnNameAndValues(Model.QueryType queryType) {
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

  public String dump() {
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

  public void fromAnnotation() {
    Object o = model;
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

  public void toAnnotation() {
    Object o = model;
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

  private Model model;

  private Map<String, Object> columnValues = new HashMap<>();

  private static final String COLUMN_NAME_ID = "id";
  private static final String COLUMN_NAME_CREATED_AT = "created_at";
  private static final String COLUMN_NAME_UPDATED_AT = "updated_at";

}