package me.zerosquare.simplemodel;

import java.util.*;
import java.lang.reflect.*;

import me.zerosquare.simplemodel.*;

@BindTable(name = "products")
public class Product extends SoftDeleteModel {
  // should be exists for update/delete
  @BindColumn(name = "id")
  public Long id;

  @BindColumn(name = "name")
  public String name;

  @BindColumn(name = "price")
  public Integer price;

  @Override
  protected void beforeExecute(QueryType type) {
    if(type == QueryType.UPDATE) {
      price = price * 10;
    }
  }
}

