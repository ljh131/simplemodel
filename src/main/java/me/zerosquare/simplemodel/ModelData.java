package me.zerosquare.simplemodel;

import java.awt.image.AreaAveragingScaleFilter;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import me.zerosquare.simplemodel.Model.QueryType;

/*
특수한 컬럼들은 아래와 같이 사용된다.
annotation이 있으면 이를 사용하고, 그렇지 않으면 preset name을 사용한다.

- id
create시 해당 이름의 컬럼으로 generated id를 받음
update/delete시 해당 이름의 컬럼을 where에 사용
find시 해당 이름의 컬럼을 where에 사용

- created_at
create시 해당 이름의 컬럼이 현재 시각으로 insert됨

- updated_at
update시 해당 이름의 컬럼이 현재 시각으로 update됨
*/
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

  public void putId(Long id) {
    idColumnValue.value(id);
    put(COLUMN_NAME_ID, id);
  }

  public Long getId() {
    if(idColumnValue.value() != null) return (Long) idColumnValue.value();

    Object o = get(COLUMN_NAME_ID);
    return o != null ? Long.parseLong(o.toString()) : null;
  }

  // FIXME return ArrayList<Pair<String. Object>>
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
    if(queryType == QueryType.INSERT) {
      colnames.add(createdAtColumnValue.column());
      colvals.add(new Timestamp(System.currentTimeMillis()));
    } else if(queryType == QueryType.UPDATE) {
      colnames.add(updatedAtColumnValue.column());
      colvals.add(new Timestamp(System.currentTimeMillis()));
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
    if(key.equals(COLUMN_NAME_ID) || 
      key.equals(COLUMN_NAME_CREATED_AT) || 
      key.equals(COLUMN_NAME_UPDATED_AT) ||
      (idColumnValue != null && key.equals(idColumnValue.column())) || 
      (createdAtColumnValue != null && key.equals(createdAtColumnValue.column())) || 
      (updatedAtColumnValue != null && key.equals(updatedAtColumnValue.column()))) {
      return false;
    }

    if(val == null) {
      //Logger.w("val is null! key '%s' is ignored for insert/update", key.toString());
      return false;
    }

    return true;
  }

  /*
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
  */

  public void fromAnnotation() {
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

          if(bc.id()) {
            // FIXME
            idColumnValue = new ColumnValue(bc.name(), val);
          } else if(bc.createdAt()) {
            // FIXME
            createdAtColumnValue = new ColumnValue(bc.name(), val);
          } else if(bc.updatedAt()) {
            // FIXME
            updatedAtColumnValue = new ColumnValue(bc.name(), val);
          }
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
      if (field.isAnnotationPresent(BindColumn.class)) {
        assertAnnotatedField(field);

        BindColumn bc = field.getAnnotation(BindColumn.class);

        try {
          Object val = get(bc.name());
          Logger.t("to annotation - %s : %s", bc.name(), val);
          setFieldValue(o, field, val);

          if(bc.id()) {
            // FIXME
            setFieldValue(o, field, idColumnValue.value());
          } else if(bc.createdAt()) {
            // FIXME
            setFieldValue(o, field, createdAtColumnValue.value());
          } else if(bc.updatedAt()) {
            // FIXME
            setFieldValue(o, field, updatedAtColumnValue.value());
          }

        } catch (IllegalArgumentException e) {
          // ignore me
          Logger.warnException(e);
        }
      }
    }
  }

  /*
  private Object getAnnotation(Function<BindColumn, Boolean> func) {
    Object o = model;
    Field[] fields = o.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.isAnnotationPresent(BindColumn.class)) {
        BindColumn bc = field.getAnnotation(BindColumn.class);

        if(func.apply(bc)) {
          try {
            Object val = field.get(o);
            return val;
          } catch (IllegalArgumentException | IllegalAccessException e) {
            // ignore me
            Logger.warnException(e);
          }
        }
      }
    }
    return null;
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

  private static final String COLUMN_NAME_ID = "id";
  private static final String COLUMN_NAME_CREATED_AT = "created_at";
  private static final String COLUMN_NAME_UPDATED_AT = "updated_at";

  private ColumnValue idColumnValue = new ColumnValue(COLUMN_NAME_ID, null);
  private ColumnValue createdAtColumnValue = new ColumnValue(COLUMN_NAME_CREATED_AT, null);
  private ColumnValue updatedAtColumnValue = new ColumnValue(COLUMN_NAME_UPDATED_AT, null);

  /*
  private Long id;
  private Timestamp createdAt;
  private Timestamp updatedAt;
  */

}