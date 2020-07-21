package me.zerosquare.simplemodel.extensions;

import me.zerosquare.simplemodel.Model;

import java.sql.Timestamp;
import java.util.List;

/**
 * Use deleted_at timestamp flag to indicate deleted row or not.
 */
public class SoftDeleteModel extends Model {

  private static final String COLUMN_NAME_DELETED_AT = "deleted_at";

  @Override
  public long delete() throws Exception {
    boolean enableBeforeExecute = setEnableBeforeHook(false);
    boolean enableAfterExecute = setEnableAfterHook(false);

    long r = updateColumn(COLUMN_NAME_DELETED_AT, new Timestamp(System.currentTimeMillis()));

    setEnableBeforeHook(enableBeforeExecute);
    setEnableAfterHook(enableAfterExecute);
    return r;
  }

  @Override
  public <T extends Model> List<T> fetch() throws Exception {
    if (!includeDeleted) {
      where(String.format("%s.%s is null", getTableName(), COLUMN_NAME_DELETED_AT));
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
