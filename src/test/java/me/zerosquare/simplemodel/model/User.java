package me.zerosquare.simplemodel.model;

import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;

@Table(name = "users")
public class User extends Model {
  @Column
  public Long id;

  @Column
  public String name;

  public User() {
  }

  public User(String name) {
      this.name = name;
  }
}

