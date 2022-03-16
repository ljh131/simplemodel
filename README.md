# simplemodel
ActiveRecord on Java for minimalist

## Install

### Use maven to install locally
```bash
mvn install -DskipTests
```

or `mvn install` if you have a testing environment

### Append dependency to your pom.xml
```xml
<dependency>
  <groupId>me.zerosquare.simplemodel</groupId>
  <artifactId>simplemodel</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Basic example 
```java
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
```

### with ORM
Declare model class for desired table
```java
@Table(name = "employees")
public class Employee extends Model {
  // special name column for convenience
  @Column
  public Long id;

  @Column(name = "company_id")
  public Long companyId;

  @Column
  public String name;

  @Column
  public Integer age;
}
```

```java
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

// update
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
```

## More examples
For more examples: https://github.com/ljh131/simplemodel/tree/master/src/test/java/me/zerosquare/simplemodel

### Join (with ORM)
```java
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

long employeeId1 = (long)rs.get(0).id;
long employeeId2 = (long)rs.get(1).id;

// joined table's columns are only accessable with their table name prefix
long companyid = rs.get(0).getInt("companies.id");
```

## Caution
Read following topics before using.

### Special columns
There are special columns have predefined column names: 

- `id`: used by find and update/delete (only if where is not specified).
  - If you want to use these methods, you should have `id` column in the table.

- `created_at`/`updated_at`: stored when row is created/updated.
  - If you want to store them, you should put `created_at`/`updated_at` with null value or map these to appropriate fields in the ORM class.

### ORM fields takes precedence
If ORM fields and data with `put` or `setColumnValues` both are exists, ORM fields takes precedence over data.

### Access joined table
Joined table's column can be accessed through `table name`.`column name`.
If you join same table multiple times, so those keys are duplicated, accessing those values may result in unexpected values. In this case, use alias to specify unique alias and access through it.

### Update column to NULL (ORM)
When you use ORM, you cannot simply assign an ORM field to null and update it. It is because null could mean not assigned. Use updateColumn instead.
