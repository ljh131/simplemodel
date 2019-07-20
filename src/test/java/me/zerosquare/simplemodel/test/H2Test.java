package me.zerosquare.simplemodel.test;

import me.zerosquare.simplemodel.Connector;
import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.internals.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class H2Test {

    @BeforeClass
    public static void tearUp() throws Exception {
        Logger.i("tear up H2Test");

        Logger.i("current path is: " + Paths.get("").toAbsolutePath().toString());

        Connector.setConnectionInfo("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa");
//        Connector.setConnectionInfo("jdbc:h2:./h2_test", "sa", "sa");

        loadSchema("db/create-simplemodel-test-table.sql");
    }

    private static void loadSchema(String filename) throws Exception {
        // load schema and create tables
        String path = "../db/" + filename;
        String schema = new String(Files.readAllBytes(Paths.get(filename)));

//        Logger.i("schema loaded: %s", schema);

        Model.execute(schema, pst -> pst.executeUpdate());
    }

    @Test
    public void testORM() throws Exception {
        String name = "simplemodel h2 tester";

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

        // exists
        assertTrue(new Employee().where("id = ?", id).exists());

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
}
