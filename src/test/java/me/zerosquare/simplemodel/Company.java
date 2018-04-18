package me.zerosquare.simplemodel;

import java.util.*;
import java.lang.reflect.*;

import me.zerosquare.simplemodel.*;

@BindTable(name = "companies")
public class Company extends Model{
  // should be exists for update/delete
  @BindColumn(name = "id")
  public Long id;

  @BindColumn(name = "name")
  public String name;

}

