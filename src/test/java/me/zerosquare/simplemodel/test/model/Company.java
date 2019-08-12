package me.zerosquare.simplemodel.test.model;

import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;

@Table(name = "companies")
public class Company extends Model {
  @Column
  public Long id;

  @Column
  public String name;

}

