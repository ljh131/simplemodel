package me.zerosquare.simplemodel;

@Table(name = "employees")
public class Employee extends Model {
  // should be exists for update/delete
  @Column
  public Long id;

  @Column(name = "company_id")
  public Long companyId;

  @Column
  public String name;

  @Column
  public Integer age;
}

