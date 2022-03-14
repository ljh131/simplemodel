package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;
import me.zerosquare.simplemodel.exceptions.AbortedException;
import me.zerosquare.simplemodel.internals.Logger;
import me.zerosquare.simplemodel.model.*;
import org.junit.*;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class SimpleModelTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Connector.setConnectionInfo("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa");

    Schema.loadSchema("src/test/resources/db/create-simplemodel-test-table.sql");
  }

  @AfterClass
  public static void afterClass() {
  }

  @Before
  public void before() {
  }

  @After
  public void after() throws Exception {
    Model.table("companies").where("1=1").delete();
    Model.table("employees").where("1=1").delete();
    Model.table("products").where("1=1").delete();

    Model.table("users").where("1=1").delete();
    Model.table("docs").where("1=1").delete();
    Model.table("comments").where("1=1").delete();
  }

  @Test
  public void testBasic() throws Exception {
    String name = makeName();
    int age = 30;

    // insert
    Model newEntry = Model.table("employees");
    newEntry.put("name", name);
    newEntry.put("age", age);
    long id = newEntry.create();
    assertTrue(id >= 1);

    // basic select (use where twice)
    //Model r = Model.table("employees").where("id = ?", id).where("name = ?", name).fetch().get(0);
    Model r = Model.table("employees").where("name = ?", name).where("age = ?", age).fetch().get(0);
    assertEquals(id, (long) r.getId());
    assertEquals(name, r.getString("name"));
    assertEquals(age, r.getInt("age"));

    long cnt = Model.table("employees").where("name = ?", name).select("count(id) cnt").fetchFirst().getLong("cnt");
    assertEquals(1, cnt);

    // update
    Model updateEntry = Model.table("employees");
    updateEntry.put("age", 31);
    assertEquals(1, updateEntry.where("id = ?", id).update());

    // findBy, find
    r = Model.table("employees").findBy("id = ?", id);
    assertEquals(31, r.getInt("age"));
    assertEquals(name, r.getString("name"));

    r = Model.table("employees").find(id);
    assertEquals(name, r.getString("name"));

    // delete with find
    assertEquals(1, Model.table("employees").find(id).delete());

    // employee select and not found
    r = Model.table("employees").find(404_404_404);
    assertNull(r);
  }

  @Test
  public void testORM() throws Exception {
    String name = makeName();

    // insert
    Employee ne = new Employee();
    ne.name = name;
    ne.age = 1;
    long id = ne.create();
    assertTrue(id >= 1);

    Logger.i("NEW EMPLOYEE ID: %d", id);

    // select
    List<Employee> es = new Employee().where("id = ?", id).fetch();
    Employee e = es.get(0);
    assertNotNull(e);
    assertEquals(name, e.name);
    assertEquals(1, e.age.intValue());

    // exists
    assertTrue(new Employee().where("id = ?", id).exists());

    // update without select
    Employee ue = new Employee();
    ue.id = id;
    ue.age = 12;
    assertEquals(1, ue.update());

    // select again
    Employee fe = new Employee().findBy("id = ?", id);
    assertEquals(12, fe.getInt("age"));

    // and delete
    Employee de = new Employee();
    de.id = id;
    assertEquals(1, de.delete());

    // confirm not exists
    assertNull(new Employee().find(id));
  }

  @Test
  public void testNotExists() throws Exception {
    assertFalse(new Employee().where("id = ?", 404_404_404).exists());
  }

  @Test
  public void testOffset() throws Exception {
    int price = (int) (System.currentTimeMillis() / 1000);

    Product np = new Product();
    np.price = price;

    for (int i = 0; i < 10; i++) {
      np.name = String.format("offset%d", i);
      assertTrue(np.create() >= 1);
    }

    Model query = new Product().where("price = ?", price).order("id").limit(5);
    List<Product> ps;

    // first page
    ps = query.offset(0).fetch();
    for (int i = 0; i < 5; i++) {
      assertEquals(String.format("offset%d", i), ps.get(i).name);
    }

    // second page
    ps = query.offset(5).fetch();
    for (int i = 0; i < 5; i++) {
      assertEquals(String.format("offset%d", i + 5), ps.get(i).name);
    }
  }

  // 한번 new해서 계속 재활용
  @Test
  public void testORMRecycled() throws Exception {
    String name = makeName();

    // insert
    Employee ne = new Employee();
    ne.name = name;
    ne.age = 1;
    long id = ne.create();
    assertTrue(id >= 1);

    Logger.i("NEW EMPLOYEE ID: %d", id);

    // select
    List<Employee> es = new Employee().where("id = ?", id).fetch();
    Employee e = es.get(0);
    assertNotNull(e);
    assertEquals(name, e.name);
    assertEquals(1, e.age.intValue());

    // update
    e.age = 12;
    assertEquals(1, e.update());

    // and delete
    assertEquals(1, e.delete());

    // confirm not exists
    assertNull(new Employee().find(id));
  }

  @Test
  public void testSoftDelete() throws Exception {
    String name = makeName();

    // insert
    Product np = new Product();
    np.name = name;
    np.price = 10;
    long id = np.create();
    assertTrue(id >= 1);

    // select
    Product p = new Product().find(id);
    assertEquals(name, p.name);

    // delete
    assertEquals(1, p.delete());

    // confirm not exists
    assertNull(new Product().find(id));

    // but it is alive actually!
    p = new Product().includeDeleted().find(id);
    assertEquals(name, p.name);
    assertNotNull(p.deletedAt);

    Logger.i("%s", p.deletedAt.toString());
  }

  @Test
  public void testBeforeExecute() throws Exception {
    String name = makeName();

    // insert
    Product np = new Product();
    np.name = name;
    np.price = 10;
    long id = np.create();
    assertTrue(id >= 1);

    // update
    assertEquals(1, np.update());

    // and price will be 10*10
    Product p = new Product().find(id);
    assertEquals(100, p.price.longValue());
  }

  @Test
  public void testJoin() throws Exception {
    // insert
    Model c = Model.table("companies");
    c.put("name", "join company");
    long cid = c.create();
    assertTrue(cid >= 1);

    // insert two
    Model e = Model.table("employees");
    e.put("name", "joined employee1");
    e.put("age", 32);
    e.put("company_id", cid);
    long eid1 = e.create();
    assertTrue(eid1 >= 1);

    e.put("name", "joined employee2");
    long eid2 = e.create();
    assertTrue(eid2 >= 1);

    // select with join
    List<Model> rs = Model.table("employees").joins("companies on companies.id = employees.company_id").where("companies.id = ?", cid).order("employees.id").fetch();

    assertEquals(2, rs.size());

    assertEquals(eid1, rs.get(0).getInt("id"));
    assertEquals(eid2, rs.get(1).getInt("id"));

    // joined table's columns are only accessable with their table name prefix
    assertEquals(cid, rs.get(0).getInt("companies.id"));
    assertEquals(cid, rs.get(1).getInt("companies.id"));
  }

  @Test
  public void testJoinORM() throws Exception {
    // insert
    Company c = new Company();
    c.name = "join company";
    long cid = c.create();
    assertTrue(cid >= 1);

    // insert two
    Employee e = new Employee();
    e.name = "joined employee1";
    e.age = 32;
    e.companyId = cid;
    long eid1 = e.create();
    assertTrue(eid1 >= 1);

    e.name = "joined employee2";
    e.age = 33;
    long eid2 = e.create();
    assertTrue(eid2 >= 1);

    // select with explicit join
    List<Employee> rs = new Employee().joins("join companies on companies.id = employees.company_id").where("companies.id = ?", cid).order("employees.id").fetch();

    assertEquals(2, rs.size());

    assertEquals(eid1, (long) rs.get(0).id);
    assertEquals(eid2, (long) rs.get(1).id);

    // joined table's columns are only accessable with their table name prefix
    assertEquals(cid, rs.get(0).getInt("companies.id"));
    assertEquals(cid, rs.get(1).getInt("companies.id"));
  }

  @Test
  public void testFindWithJoin() throws Exception {
    // insert
    Company c = new Company();
    c.name = "join company";
    long cid = c.create();
    assertTrue(cid >= 1);

    // insert two
    Employee e = new Employee();
    e.name = "joined employee1";
    e.age = 32;
    e.companyId = cid;
    long eid1 = e.create();
    assertTrue(eid1 >= 1);

    e.name = "joined employee2";
    e.age = 33;
    long eid2 = e.create();
    assertTrue(eid2 >= 1);

    Employee rs = new Employee().joins("join companies on companies.id = employees.company_id").find(eid1);
    assertEquals(eid1, (long) rs.id);
  }

  /**
   * double age when save
   * abort when age is zero
   */
  @Table(name = "employees")
  public static class MyEmployee extends Model {

    @Column
    public Long id;

    @Column(name = "company_id")
    public Long companyId;

    @Column
    public String name;

    @Column
    public Integer age;

    @Override
    protected void beforeExecute(QueryType type) throws AbortedException {
      if (age == 0) throw new AbortedException();

      // double age before save
      if (type == QueryType.INSERT || type == QueryType.UPDATE) {
        age *= 2;
      }
    }

    @Override
    protected void afterExecute(QueryType type, boolean success) throws AbortedException {
      if (age == 0) throw new AbortedException();

      // half age after select
      if (type == QueryType.SELECT) {
        age /= 2;
      }
    }

    public void disableHandlers() {
      setEnableBeforeHook(false);
      setEnableAfterHook(false);
    }
  }

  @Test
  public void testBeforeAndAfterExecute() throws Exception {
    String name = makeName();
    int age = 32;

    MyEmployee me = new MyEmployee();
    me.name = name;
    me.age = age;
    long eid = me.create();
    assertTrue(eid >= 1);

    // check whether age in db is doubled
    Employee e = new Employee().find(eid);
    assertEquals(name, e.name);
    assertEquals(age * 2, (long) e.age);

    MyEmployee me2 = new MyEmployee().find(eid);
    assertEquals(age, (long) me2.age);

    // fail to create when age is zero
    age = 0;
    me.name = name;
    me.age = age;
    try {
      eid = me.create();
      fail();
    } catch (AbortedException ex) {
      assertNotNull(ex);
    }

    // insert by force
    e = new Employee();
    e.name = name;
    e.age = age;
    eid = e.create();
    assertTrue(eid >= 1);

    // fail to get when age is zero
    try {
      new MyEmployee().find(eid);
      fail();
    } catch (AbortedException ex) {
      assertNotNull(ex);
    }

    // now, disable handler and insert with age zero
    me.disableHandlers();
    me.age = 0;
    me.create();
  }

  @Test
  public void testManualQuery() throws Exception {
    String result = Model.execute("select 'hello simplemodel'", pst -> {
      ResultSet rs = pst.executeQuery();
      rs.next();
      return rs.getString(1);
    });

    assertEquals("hello simplemodel", result);
  }

  @Test
  public void testSubclassORM() throws Exception {
    String name = makeName();

    // insert
    DummyEmployee ne = new DummyEmployee();
    ne.name = name;
    ne.age = 1;
    long id = ne.create();
    assertTrue(id >= 1);

    Logger.i("NEW EMPLOYEE ID: %d", id);

    // find
    DummyEmployee e = new DummyEmployee().find(id);
    assertNotNull(e);
    assertEquals(name, e.name);
    assertEquals(1, e.age.intValue());
  }

  @Test
  public void testUpdateModifiedColumnsOnly() throws Exception {
    Employee e = new Employee();
    e.name = "original name";
    e.companyId = 1L;
    e.age = 1;

    // create and update modified
    long id = e.create();
    assertTrue(id >= 1);

    e.companyId = 2L;
    e.age = 2;
    e.update(true);

    // select and update modified
    e = new Employee().find(id);
    assertEquals("original name", e.name);
    assertEquals(2L, (long) e.companyId);
    assertEquals(2, (int) e.age);

    e.companyId = 3L;
    e.age = 3;
    e.update(true);

    // update twice and update modified
    e = new Employee().find(id);
    assertEquals("original name", e.name);
    assertEquals(3L, (long) e.companyId);
    assertEquals(3, (int) e.age);

    e.companyId = 4L;
    e.age = 4;
    e.update(true);

    e.companyId = 5L;
    e.age = 5;
    e.update(true);

    e = new Employee().find(id);
    assertEquals("original name", e.name);
    assertEquals(5L, (long) e.companyId);
    assertEquals(5, (int) e.age);
  }

  @Test
  public void testGetModifiedColumnValues() {
    ModelData data = new ModelData();
    data.put("a", 1);
    data.put("b", 2);
    data.put("c", 3);
    data.put("n", null);

    data.saveColumnValues();

    data.put("b", 22);
    data.put("c", 33);

    Map<String, Object> modifiedColumnValues = data.getModifiedColumnValues();

    assertEquals(2, modifiedColumnValues.size());
    assertEquals(22, modifiedColumnValues.get("b"));
    assertEquals(33, modifiedColumnValues.get("c"));
    assertNull(modifiedColumnValues.get("a"));
  }

  @Test
  public void testTransactionCommit() throws Exception {
    AtomicLong eid = new AtomicLong();

    Transaction.execute(() -> {
      Company c = new Company();
      c.name = "tranx company";
      long cid = c.create();

      Employee e = new Employee();
      e.name = "tranx";
      e.companyId = cid;
      e.age = 100;

      // create and update modified
      eid.set(e.create());
      assertTrue(eid.get() >= 1);
    });

    // verify
    Employee e = new Employee().find(eid.get());
    assertEquals("tranx", e.name);
    assertEquals(100, (int) e.age);
  }

  @Test
  public void testTransactionRollback() throws Exception {
    AtomicLong cid = new AtomicLong();
    AtomicLong eid = new AtomicLong();

    Exception coughte = null;

    try {
      Transaction.execute(() -> {
        Company c = new Company();
        c.name = "roll company";
        cid.set(c.create());

        Employee e = new Employee();
        e.name = "roll";
        e.companyId = cid.get();
        e.age = 100;

        // create and update modified
        eid.set(e.create());
        assertTrue(eid.get() >= 1);

        throw new Exception("abort tranx");
      });
    } catch (Exception ex) {
      coughte = ex;
    }

    assertEquals("abort tranx", coughte.getMessage());

    // verify
    Company c = new Company().find(cid.get());
    assertNull(c);

    Employee e = new Employee().find(eid.get());
    assertNull(e);
  }

  @Test
  public void testColumnAlias() throws Exception {
    User u = new User("u");
    u.id = u.create();

    User r = new User().select("name n").find(u.id);
    assertEquals("u", r.name);
    assertEquals("u", r.get("n"));
  }

  @Test
  public void testJoinSameTableMultiple() throws Exception {
    User u = new User("u");
    u.id = u.create();

    User u2 = new User("u2");
    u2.id = u2.create();

    Doc d = new Doc(u.id, "title", "content");
    d.id = d.create();

    // u2 write comment on u's doc
    Comment c = new Comment(u2.id, d.id, "comment");
    c.id = c.create();

    List<Comment> rs = new Comment()
            .joins("LEFT OUTER JOIN users on users.id = comments.user_id")
            .joins("LEFT OUTER JOIN docs on docs.id = comments.doc_id")
            .joins("LEFT OUTER JOIN users AS doc_users on doc_users.id = docs.user_id")
            .select("*, doc_users.name as doc_users_name")
            .fetch();

    Comment r = rs.get(0);

    assertEquals(u.name, r.get("doc_users_name"));
    assertEquals(u2.name, r.get("users.name"));
  }

  @Test
  public void testUpdateColumnToNull() throws Exception {
    User u = new User("u");
    u.id = u.create();

    Doc d = new Doc(u.id, "title", "content");
    d.meta = "test meta";
    d.id = d.create();

    Doc ds = new Doc().find(d.id);
    assertEquals("test meta", ds.meta);

    // update to null and get
    d.updateColumn("meta", null);

    ds = new Doc().find(d.id);
    assertEquals(null, ds.meta);
  }

  private String makeName() {
    return UUID.randomUUID().toString();
  }
}
