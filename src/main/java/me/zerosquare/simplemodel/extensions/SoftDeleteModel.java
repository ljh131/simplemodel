package me.zerosquare.simplemodel.extensions;

import me.zerosquare.simplemodel.Model;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class SoftDeleteModel extends Model {
  @Override
  public long delete() throws SQLException, InterruptedException {
    return updateColumn("deleted_at", new Timestamp(System.currentTimeMillis()));
  }

  @Override
  public <T extends Model> List<T> fetch() throws SQLException, InterruptedException {
    if(!includeDeleted) {
      where(String.format("%s.deleted_at is null", getTableName()));
    }
    return super.fetch();
  }

  public <T extends Model> T includeDeleted() {
    return includeDeleted(true);
  }

  public <T extends Model> T includeDeleted(boolean flag) {
    includeDeleted = flag;
    return (T)this;
  }

  private boolean includeDeleted = false;

}
