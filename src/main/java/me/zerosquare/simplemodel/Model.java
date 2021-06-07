package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.annotations.Table;
import me.zerosquare.simplemodel.exceptions.ConstructionException;
import me.zerosquare.simplemodel.internals.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Use this class directly, or extends this class to use ORM.
 */
public class Model {

  String tableName;

  ModelData data = new ModelData();

  private String reservedWhere = "";
  private ArrayList<Object> reservedWhereParams = new ArrayList<>();

  private String reservedSelect = "";
  private String reservedJoin = "";
  private String reservedOrderby = "";
  private String reservedLimit = "";
  private String reservedOffset = "";

  public static Model table(String tableName) {
    return new Model(tableName);
  }

  public Model(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  public Model() {
    trySetTableNameFromAnnotation();
  }

  private void trySetTableNameFromAnnotation() {
    for (Class c = this.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
      if (c.isAnnotationPresent(Table.class)) {
        Annotation annotation = c.getAnnotation(Table.class);
        Table bc = (Table) annotation;
        this.tableName = bc.name().toLowerCase();
        break;
      }
    }
  }

  public enum QueryType {
    INSERT,
    SELECT,
    UPDATE,
    DELETE
  }

  /**
   * @return generatedId if exists, otherwise 0
   */
  public long create() throws Exception {
    QueryType queryType = QueryType.INSERT;
    _beforeExecute(queryType);

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.INSERT, false);
    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    String q = String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, 
      StringUtils.join(colnames.toArray(), ','), 
      StringUtils.join(colnames.stream().map(e -> "?").toArray(), ','));

    return execute(queryType, q, pst -> {
      addParameters(pst, 0, colvals);

      pst.executeUpdate();
      ResultSet rs = pst.getGeneratedKeys();
      if (rs.next()) {
        long generatedId = rs.getLong(1);

        data.putId(generatedId);

        return ExecuteResult.of(true, generatedId);
      }

      return ExecuteResult.of(true, 0L);
    });
  }

  public <T extends Model> T select(String selectClause, Object... args) {
    reservedSelect = String.format(selectClause, args);
    return (T)this;
  }

  private static final Pattern findJoinPattern = Pattern.compile("\\bjoin\\b", Pattern.CASE_INSENSITIVE);

  public <T extends Model> T joins(String joinClause, Object... args) {
    if (!findJoinPattern.matcher(joinClause).find()) {
      joinClause = "JOIN " + joinClause;
    }

    String c = String.format(joinClause, args);
    reservedJoin += " " + c;
    return (T)this;
  }

  public <T extends Model> T order(String orderClause, Object... args) {
    reservedOrderby = String.format(orderClause, args);
    return (T)this;
  }

  public <T extends Model> T limit(long limitNumber) {
    reservedLimit = String.format("%d", limitNumber);
    return (T)this;
  }

  public <T extends Model> T offset(long offsetNumber) {
    reservedOffset = String.format("%d", offsetNumber);
    return (T)this;
  }

  /**
   * Set where clause.
   * If you don't set where clause, default where clause will be used. (id=?)
   * If you set where clause, default where clause won't be used.
   * @param whereClause
   * @param args
   * @param <T>
   * @return
   */
  public <T extends Model> T where(String whereClause, Object... args) {
    if (StringUtils.isBlank(whereClause)) return (T)this;

    reservedWhereParams.addAll(Arrays.asList(args));

    if (reservedWhere.isEmpty()) {
      reservedWhere = whereClause;
    } else {
      reservedWhere += " and " + whereClause;
    }
    return (T)this;
  }

  /**
   * @return empty list if no result found
   */
  public <T extends Model> List<T> fetch() throws Exception {
    QueryType queryType = QueryType.SELECT;

    String q = String.format("SELECT %s from %s", 
        reservedSelect.isEmpty() ? "*" : reservedSelect, 
        tableName);
    if (!reservedJoin.isEmpty()) {
      q += String.format(" %s", reservedJoin);
    }
    if (!reservedWhere.isEmpty()) {
      q += String.format(" WHERE %s", reservedWhere);
    }
    if (!reservedOrderby.isEmpty()) {
      q += String.format(" ORDER BY %s", reservedOrderby);
    }
    if (!reservedLimit.isEmpty()) {
      q += String.format(" LIMIT %s", reservedLimit);
    }
    if (!reservedOffset.isEmpty()) {
      q += String.format(" OFFSET %s", reservedOffset);
    }

    return execute(queryType, q, pst -> {
      addParameters(pst, 0, reservedWhereParams);
      ResultSet rs = pst.executeQuery();
      ArrayList<T> models = new ArrayList<>();

      while(rs.next()) {
        Map<String, Object> colvals = data.getColumnValuesFromResultSet(tableName, rs);

        T model = newInstance();
        model.tableName = tableName;
        model.data.setColumnValues(colvals);
        model._afterExecute(queryType, true);

        models.add(model);
      }

      return ExecuteResult.of(true, (List<T>)models);
    });
  }

  /**
   * Fetch first row only.
   * This method is helpful for these kind of queries - `select count(id) ...`.
   * Note that it does not use - `limit 1` query.
   */
  public <T extends Model> T fetchFirst() throws Exception {
    List<Model> fetched = fetch();
    return fetched.isEmpty() ? null : (T)fetched.get(0);
  }

  /**
   * @return null if no result
   */
  public <T extends Model> T findBy(String whereClause, Object... args) throws Exception {
    List<T> r = where(whereClause, args).limit(1).fetch();
    if (r == null || r.isEmpty()) return null;
    return r.get(0);
  }

  /**
   * @return null if no result
   */
  public <T extends Model> T find(long id) throws Exception {
    return findBy(makeWhereWithFindId(id));
  }

  public boolean exists() throws Exception {
      select("1");
      limit(1);
      return fetchFirst() != null;
  }

  /**
   * Update all columns.
   * @return affected row count
   */
  public long update() throws Exception {
      return update(false);
  }

  /**
   * update all columns or only modified columns.
   * @param updateModifiedOnly
   * @return
   * @throws Exception
   */
  public long update(boolean updateModifiedOnly) throws Exception {
    QueryType queryType = QueryType.UPDATE;
    _beforeExecute(queryType);

    Pair<ArrayList<String>, ArrayList<Object>> nvs = data.buildColumnNameAndValues(QueryType.UPDATE, updateModifiedOnly);

    ArrayList<String> colnames = nvs.getLeft();
    ArrayList<Object> colvals = nvs.getRight();

    if (colnames.isEmpty()) {
      Logger.w("nothing to update");
      return 0;
    }

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName,
            StringUtils.join(colnames.stream().map(c -> String.format("%s=?", c)).toArray(), ','),
            getReservedWhere());

    return execute(queryType, q, pst -> {
      int last = addParameters(pst, 0, colvals);
      addParameters(pst, last, reservedWhereParams);

      return ExecuteResult.of(true, (long)pst.executeUpdate());
    });
  }

  /**
   * Update single column only.
   * Note that it update by using only specified column name and value, not using ORM or other values.
   * @return affected row count
   */
  public long updateColumn(String columnName, Object value) throws Exception {
    QueryType queryType = QueryType.UPDATE;
    _beforeExecute(queryType);

    String q = String.format("UPDATE %s SET %s WHERE %s", tableName,
            String.format("%s=?", columnName),
            getReservedWhere());

    return execute(queryType, q, pst -> {
      int last = addParameters(pst, 0, Arrays.asList(value));
      addParameters(pst, last, reservedWhereParams);

      return ExecuteResult.of(true, (long)pst.executeUpdate());
    });
  }

  /**
   * @return affected row count
   */
  public long delete() throws Exception {
    QueryType queryType = QueryType.DELETE;
    _beforeExecute(queryType);

    String q = String.format("DELETE FROM %s WHERE %s", tableName,
      getReservedWhere());

    return execute(queryType, q, pst -> {
      addParameters(pst, 0, reservedWhereParams);

      return ExecuteResult.of(true, (long)pst.executeUpdate());
    });
  }

  public void setColumnValues(Map<String, Object> colvals) {
    data.setColumnValues(colvals);
  }

  public Map<String, Object> getColumnValues() {
    return data.getColumnValues();
  }

  /**
   * Put column and value for create/update
   */
  public <T extends Model> T put(String key, Object val) {
    data.put(key, val);
    return (T)this;
  }

  /**
   * Get column value
   */
  public Object get(String columnName) {
    return data.get(columnName);
  }

  public String getString(String columnName) {
    return (String)get(columnName);
  }

  public int getInt(String columnName) {
    return (int)get(columnName);
  }

  public long getLong(String columnName) {
    return (long)get(columnName);
  }

  public Long getId() {
    return data.getId();
  }

  public String getTableName() {
    return tableName;
  }

  private boolean enableBeforeExecute = true;

  private boolean enableAfterExecute = true;

  /**
   * Enable/disable handler before query execution
   * @param enable
   * @return old value
   */
  public boolean setEnableBeforeHook(boolean enable) {
      boolean old = enableBeforeExecute;
      enableBeforeExecute = enable;
      return old;
  }

  /**
   * Enable/disable handler after query execution
   * @param enable
   * @return old value
   */
  public boolean setEnableAfterHook(boolean enable) {
    boolean old = enableAfterExecute;
    enableAfterExecute = enable;
    return old;
  }

  /**
   * Disable before/after hooks
   */
  public <T extends Model> T disableHooks() {
    setEnableBeforeHook(false);
    setEnableAfterHook(false);
    return (T)this;
  }

  /**
   * Override this method to handle something before query execution or abort the execution.
   * Note that select does not invoke this method.
   * @throws Exception throw any exception to interrupt execution
   */
  protected void beforeExecute(QueryType type) throws Exception { }

  /**
   * Override this method to handle something after query execution or abort the execution.
   * Note that even if you abort the execution, already executed query is not rolled back.
   * @throws Exception throw any exception to interrupt execution
   */
  protected void afterExecute(QueryType type, boolean success) throws Exception { }

  void _beforeExecute(QueryType queryType) throws Exception {
    Logger.t("before execute: %s", queryType.name());

    if (enableBeforeExecute) {
      beforeExecute(queryType);
    }

    data.columnValuesFromAnnotation(this);

    if (queryType == QueryType.UPDATE || queryType == QueryType.DELETE) {
      reserveDefaultWhereForUpdate();
    }
  }

  void _afterExecute(QueryType queryType, boolean success) throws Exception {
    Logger.t("after execute: %s %s", queryType.name(), success);

    if (success) {
      data.columnValuesToAnnotation(this);
      data.saveColumnValues();
    }

    if (enableAfterExecute) {
      afterExecute(queryType, success);
    }
  }

  private <T> T newInstance() throws ConstructionException {
    Constructor<? extends Model> declaredConstructor;

    try {
      declaredConstructor = getClass().getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      throw new ConstructionException("cannot find constructor for this class");
    }

    try {
      return (T) declaredConstructor.newInstance();
    } catch(InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new ConstructionException("cannot make new instance of this class");
    }
  }

  /**
   * @return last set column index
   */
  private int addParameters(PreparedStatement pst, int lastColumnIndex, List<Object> vals) throws SQLException {
    int colidx = 0;

    for(int i = 0; i < vals.size(); i++) {
      Object val = vals.get(i);
      colidx = lastColumnIndex + 1 + i;

      if (val == null) {
        Logger.t("preparams - idx: %d colidx: %d val: (null)", i, colidx);
        continue;
      }

      Logger.t("preparams - idx: %d colidx: %d val: %s", i, colidx, val.toString());

      // TODO need more
      if (val instanceof Integer) {
        pst.setInt(colidx, (Integer)val);
      } else if (val instanceof Long) {
        pst.setLong(colidx, (Long)val);
      } else if (val instanceof Boolean) {
        pst.setBoolean(colidx, (Boolean)val);
      } else if (val instanceof String) {
        pst.setString(colidx, (String)val);
      } else if (val instanceof Timestamp) {
        pst.setTimestamp(colidx, (Timestamp)val);
      } else if (val instanceof java.sql.Date) {
        pst.setDate(colidx, (java.sql.Date)val);
      } else if (val instanceof LocalDate) {
        pst.setDate(colidx, java.sql.Date.valueOf((LocalDate)val));
      } else {
        Logger.w("preparams - unrecognize type for val: %s", val.toString());
      }
    }
    return colidx;
  }

  private void reserveDefaultWhereForUpdate() {
    if (!StringUtils.isBlank(reservedWhere)) return;
    reservedWhere = makeDefaultWhereForUpdate();
    Logger.t("default where for update reserved: %s", reservedWhere);
  }

  // update/delete시 조건문을 지정하지 않을 경우 사용하는 where절
  private String makeDefaultWhereForUpdate() {
    Long id = data.getId();
    return makeWhereWithFindId(id);
  }

  private String makeWhereWithFindId(long id) {
    return StringUtils.isBlank(reservedJoin) ?
      String.format("id=%d", id) :
      String.format("%s.id=%d", tableName, id);
  }

  private String getReservedWhere() {
    if (StringUtils.isBlank(reservedWhere)) {
      throw new IllegalArgumentException("no where clause specified!");
    }
    return reservedWhere;
  }


  /*
   * internal query execution
   */

  @FunctionalInterface
  public interface StatementExecutor<R> {
    ExecuteResult<R> execute(PreparedStatement pst) throws Exception;
  }

  public static class ExecuteResult<R> {
    public static <R> ExecuteResult<R> of(R result) {
        return new ExecuteResult<>(true, result);
    }

    public static <R> ExecuteResult<R> of(boolean success, R result) {
      return new ExecuteResult<>(success, result);
    }

    private ExecuteResult(boolean succees, R result) {
      this.success = succees;
      this.result = result;
    }

    private ExecuteResult() {
      this(false, null);
    }

    public boolean isSucceed() {
      return success;
    }

    public R getResult() {
      return result;
    }

    private boolean success;
    private R result;
  }

  private <R> R execute(QueryType queryType, String sql, StatementExecutor<R> exec) throws Exception {
    ExecuteResult<R> result = new ExecuteResult<>();
    Connector c = null;
    boolean success = false;

    try {
      c = Connector.prepareStatement(sql, true);
      PreparedStatement pst = c.getPreparedStatement();

      result = exec.execute(pst);

      success = true;
    } catch(SQLException e) {
      Logger.e("fail to execute - %s", Logger.getExceptionString(e));
      throw e;
    } finally {
      if (c != null) { c.executed(success); }
      if (queryType != null && queryType != QueryType.SELECT) {
        _afterExecute(queryType, result.isSucceed());
      }
    }
    return result.getResult();
  }

  @FunctionalInterface
  public interface ManualExecuteFunction<R> {
    R call(PreparedStatement pst) throws Exception;
  }

  /**
   * Execute SQL in old way
   * @param sql
   * @param exec executable function with PreparedStatement.
   * @return returns result from the return value of exec
   */
  public static <R> R execute(String sql, ManualExecuteFunction<R> exec) throws Exception {
    R result = null;
    Connector c = null;
    boolean success = false;

    try {
      c = Connector.prepareStatement(sql, true);
      PreparedStatement pst = c.getPreparedStatement();

      result = exec.call(pst);

      success = true;
    } catch(SQLException e) {
      Logger.e("fail to execute - %s", Logger.getExceptionString(e));
      throw e;
    } finally {
      if (c != null) { c.executed(success); }
    }
    return result;
  }

}
