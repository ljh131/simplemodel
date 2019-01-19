package me.zerosquare.simplemodel;

import me.zerosquare.simplemodel.internal.Connector;
import me.zerosquare.simplemodel.internal.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleModelTest {
  @BeforeClass
  public static void tearUp() {
    Logger.i("tear up SimpleModelTest");
    Connector.setConnectionInfo("jdbc:mysql://localhost/simplemodel?useSSL=false", "simplemodeluser", "simplemodeluserpw");
  }

  @AfterClass
  public static void tearDown() {
    Logger.i("tear down SimpleModelTest");
    // TODO clear table;
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
    assertEquals(id, (long)r.getId());
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
    r = Model.table("employees").find(44444444);
    assertEquals(null, r);
  }

  @Test
  public void testUpdateColumn() throws SQLException {
    String name = makeName();

    // insert
    Model newEntry = Model.table("employees");
    newEntry.put("name", name);
    newEntry.put("age", 30);
    long id = newEntry.create();
    assertTrue(id >= 1);

    // update specific column only
    Model.table("employees").where("id=?", id).updateColumn("age", 11);

    Model r = Model.table("employees").find(id);
    assertEquals(name, r.getString("name"));
    assertEquals(11, r.getInt("age"));

    // again (with find, not where)
    Model.table("employees").find(id).updateColumn("age", 22);

    r = Model.table("employees").find(id);
    assertEquals(22, r.getInt("age"));
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
    assertTrue(e != null);
    assertEquals(name, e.name);
    assertEquals(1, e.age.intValue());

    // update
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
    assertTrue(new Employee().find(id) == null);
  }

  @Test
  public void testOffset() throws SQLException {
    int price = (int)(System.currentTimeMillis() / 1000);

    Product np = new Product();
    np.price = price;

    for(int i = 0; i < 10; i++) {
      np.name = String.format("offset%d", i);
      assertTrue(np.create() >= 1);
    }

    Model query = new Product().where("price = ?", price).order("id").limit(5);
    List<Product> ps;

    // first page
    ps = query.offset(0).fetch();
    for(int i = 0; i < 5; i++) {
      assertEquals(String.format("offset%d", i), ps.get(i).name);
    }

    // second page
    ps = query.offset(5).fetch();
    for(int i = 0; i < 5; i++) {
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
    assertTrue(e != null);
    assertEquals(name, e.name);
    assertEquals(1, e.age.intValue());

    // update
    e.age = 12;
    assertEquals(1, e.update());

    // and delete
    assertEquals(1, e.delete());

    // confirm not exists
    assertTrue(new Employee().find(id) == null);
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
    assertTrue(p.delete() == 1);

    // confirm not exists
    assertTrue(new Product().find(id) == null);

    // but it is alive actually!
    p = new Product().includeDeleted().find(id);
    assertEquals(name, p.name);
    assertTrue(p.deletedAt != null);

    Logger.i("%s", p.deletedAt.toString());
  }

  @Test
  public void testBeforeExecute() throws Exception{
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
  public void testJoin() throws SQLException {
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
  public void testJoinORM() throws SQLException {
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

    assertEquals(eid1, (long)rs.get(0).id);
    assertEquals(eid2, (long)rs.get(1).id);

    // joined table's columns are only accessable with their table name prefix
    assertEquals(cid, rs.get(0).getInt("companies.id"));
    assertEquals(cid, rs.get(1).getInt("companies.id"));
  }

  @Test
  public void testFindWithJoin() throws SQLException {
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
    assertEquals(eid1, (long)rs.id);
  }

  private String makeName() {
    return UUID.randomUUID().toString();
  }
}
