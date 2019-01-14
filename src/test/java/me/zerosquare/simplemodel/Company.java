package me.zerosquare.simplemodel;

@Table(name = "companies")
public class Company extends Model{
  // should be exists for update/delete
  @Column
  public Long id;

  @Column
  public String name;

}

