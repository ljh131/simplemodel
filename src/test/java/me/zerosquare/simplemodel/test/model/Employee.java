package me.zerosquare.simplemodel.test.model;

import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;

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

