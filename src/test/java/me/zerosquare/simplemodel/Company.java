package me.zerosquare.simplemodel;

@Table(name = "companies")
public class Company extends Model{
  @Column
  public Long id;

  @Column
  public String name;

}

