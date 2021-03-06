package me.zerosquare.simplemodel.model;

import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;
import me.zerosquare.simplemodel.extensions.SoftDeleteModel;

import java.sql.*;

@Table(name = "products")
public class Product extends SoftDeleteModel {
  @Column
  public Long id;

  @Column(name = "company_id")
  public Long companyId;

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
    if (type == QueryType.UPDATE) {
      price = price * 10;
    }
  }
}
