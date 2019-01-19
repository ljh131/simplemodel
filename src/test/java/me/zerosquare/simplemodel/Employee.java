package me.zerosquare.simplemodel;

@Table(name = "employees")
public class Employee extends Model {
  @Column
  public Long id;

  @Column(name = "company_id")
  public Long companyId;

  @Column
  public String name;

  @Column
  public Integer age;
}

