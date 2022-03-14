package me.zerosquare.simplemodel.example;

import me.zerosquare.simplemodel.Connector;
import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.Schema;
import me.zerosquare.simplemodel.model.Company;
import me.zerosquare.simplemodel.model.Employee;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class SimpleModelExample {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Connector.setConnectionInfo("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa");

    Schema.loadSchema("src/test/resources/db/create-simplemodel-test-table.sql");
  }

  @AfterClass
  public static void afterClass() {
  }

  @Test
  public void testExample() throws Exception {
    /*
     * basic example without ORM
     */

    // insert
    long id = Model.table("employees")
            .put("name", "simplemodel tester")
            .put("age", 30)
            .create();

    System.out.printf("new employee created with id: %d\n", id);

    // select (using where)
    Model r = Model.table("employees")
            .where("name = ?", "simplemodel tester")
            .where("age = ?", 30)
            .fetch().get(0);

    System.out.printf("selected employee id: %d, name: %s, age: %d\n",
            r.getId(), r.getString("name"), r.getInt("age"));

    // update
    long numUpdated = Model.table("employees")
            .where("id = ?", id)
            .put("age", 31)
            .update();

    // you could use findBy (like rails' Active Record)
    r = Model.table("employees")
            .findBy("name = ? and age = ?", "simplemodel tester", 31);

    System.out.printf("selected employee id: %d, name: %s, age: %d\n",
            r.getId(), r.getString("name"), r.getInt("age"));

    // or more simply, find with id
    r = Model.table("employees").find(id);

    // and delete
    Model.table("employees").find(id).delete();
  }

  @Test
  public void testExample2() throws Exception {
    /*
     * basic example with ORM
     */

    // insert
    Employee ne = new Employee();
    ne.name = "simplemodel orm tester";
    ne.age = 22;
    long id = ne.create();

    // select
    List<Employee> es = new Employee().where("id = ?", id).fetch();
    Employee e = es.get(0);
    System.out.printf("selected employee id: %d, name: %s, age: %d\n",
            e.id, e.name, e.age);

    // update without select
    Employee ue = new Employee();
    ue.id = id;
    ue.age = 12;
    ue.update();

    // you can use find or findBy
    Employee fe = new Employee().find(id);
    Employee fe2 = new Employee().findBy("name = ? and age = ?", "simplemodel orm tester", 22);

    // delete
    Employee de = new Employee();
    de.id = id;
    de.delete();
  }

  @Test
  public void testExample3() throws Exception {
    /*
     * more examples
     */

    // join

    // insert company
    Company c = new Company();
    c.name = "join company";
    long cid = c.create();

    // insert two joined employees
    Employee e = new Employee();
    e.name = "joined employee1";
    e.age = 32;
    e.companyId = cid;
    e.create();

    e.name = "joined employee2";
    e.age = 33;
    e.create();

    // select with join
    List<Employee> rs = new Employee()
            .joins("companies on companies.id = employees.company_id")
            .where("companies.id = ?", cid)
            .order("employees.id").fetch();

    long employeeId1 = rs.get(0).id;
    long employeeId2 = rs.get(1).id;

    // joined table's columns are only accessable with their table name prefix
    long companyid = rs.get(0).getInt("companies.id");
  }

}
