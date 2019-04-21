package me.zerosquare.simplemodel.test;

import me.zerosquare.simplemodel.Column;
import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.Table;

@Table(name = "companies")
public class Company extends Model {
  @Column
  public Long id;

  @Column
  public String name;

}

