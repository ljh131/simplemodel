package me.zerosquare.simplemodel;

import java.util.*;
import java.sql.*;

public class SoftDeleteModel extends Model {
  @Override
  public int delete() {
    return updateColumn("deleted_at", new Timestamp(System.currentTimeMillis()));
  }

  @Override
  public <T extends Model> List<T> fetch() {
    if(!includeDeleted) {
      where(String.format("%s.deleted_at is null", getTableName()));
    }
    return super.fetch();
  }

  public Model includeDeleted() {
    return includeDeleted(true);
  }

  public Model includeDeleted(boolean flag) {
    includeDeleted = flag;
    return this;
  }

  private boolean includeDeleted = false;

}
