package me.zerosquare.simplemodel;

import java.util.*;
import java.sql.*;

public class SoftDeleteModel extends Model {
  @Override
  public int delete(String whereClause, Object... args) {
    put("deleted_at", new Timestamp(System.currentTimeMillis()));
    return update(whereClause, args);
  }

  @Override
  public <T extends Model> List<T> fetch() {
    if(!includeDeleted) {
      where("deleted_at is null");
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
