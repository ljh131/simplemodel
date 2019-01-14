package me.zerosquare.simplemodel;

import java.sql.*;

@Table(name = "products")
public class Product extends SoftDeleteModel {
  // should be exists for update/delete
  @Column
  public Long id;

  @Column
  public String name;

  @Column
  public Integer price;

  @Column(name = "deleted_at")
  public Timestamp deletedAt;

  @Override
  protected void beforeExecute(QueryType type) {
    if(type == QueryType.UPDATE) {
      price = price * 10;
    }
  }
}
