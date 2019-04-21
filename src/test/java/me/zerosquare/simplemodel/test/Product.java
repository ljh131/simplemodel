package me.zerosquare.simplemodel.test;

import me.zerosquare.simplemodel.Column;
import me.zerosquare.simplemodel.SoftDeleteModel;
import me.zerosquare.simplemodel.Table;

import java.sql.*;

@Table(name = "products")
public class Product extends SoftDeleteModel {
  @Column
  public Long id;

  @Column
  public String name;

  @Column
  public Integer price;

  @Column(name = "created_at")
  public Timestamp createdAt;

  @Column(name = "updated_at")
  public Timestamp updatedAt;

  @Column(name = "deleted_at")
  public Timestamp deletedAt;

  @Override
  protected void beforeExecute(QueryType type) {
    if(type == QueryType.UPDATE) {
      price = price * 10;
    }
  }
}
