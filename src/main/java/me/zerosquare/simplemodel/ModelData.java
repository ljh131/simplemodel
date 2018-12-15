package me.zerosquare.simplemodel;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

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

  public Pair<ArrayList<String>, ArrayList<Object>> buildColumnNameAndValues() {
    ArrayList<String> colnames = new ArrayList<>();
    ArrayList<Object> colvals = new ArrayList<>();

    for (Map.Entry<String, Object> e : columnValues.entrySet()) {
      String key = e.getKey();
      Object val = e.getValue();
      if(!isValidKeyValue(key, val)) continue;

      colnames.add(key);
      colvals.add(val);
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

  // 유효하지 않은 k/v가 insert/update되지 않도록 스킵
  private boolean isValidKeyValue(String key, Object val) {
    // FIXME
    if(key.equals("id") || key.equals("created_at")) {
      return false;
    }

    if(val == null) {
      //Logger.w("val is null! key '%s' is ignored for insert/update", key.toString());
      return false;
    }

    return true;
  }

  private Long getIdFromAnnotation() {
    Object o = this;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        BindColumn bc = field.getAnnotation(BindColumn.class);
        
        if(bc.id()) {
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

  private void fromAnnotation() {
    Object o = model;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        assertAnnotatedField(field);

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

  private void toAnnotation() {
    Object o = model;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        assertAnnotatedField(field);

        BindColumn bc = field.getAnnotation(BindColumn.class);

        try {
          Object val = get(bc.name());
          Logger.t("to annotation - %s : %s", bc.name(), val);
          setFieldValue(o, field, val);
        } catch (IllegalArgumentException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  /*
  private void fillSingleObjectToAnnotation(String name, Object val) {
    Object o = model;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        BindColumn bc = field.getAnnotation(BindColumn.class);

        if(bc.name().equals(name)) {
          Logger.t("to annotation - %s : %s", name, val);
          setFieldValue(o, field, val);
        }
      }
    }
  }
  */

  // Integer를 Long에 넣을 수 있도록 한다.
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

  private Long id;
  private Timestamp createdAt;
  private Timestamp updatedAt;

}