package me.zerosquare.simplemodel;

public class ColumnValue {
  private String column;
  private Object value;

  public ColumnValue(String column, Object value) {
    this.column = column;
    this.value = value;
  }

  public void column(String col) {
    column = col;
  }

  public String column() {
    return column;
  }

  public void value(Object val) {
    value = val;
  }

  public Object value() {
    return value;
  }
}