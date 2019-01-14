package me.zerosquare.simplemodel;

import java.util.*;
import java.lang.reflect.*;

import me.zerosquare.simplemodel.*;

@BindTable(name = "employees")
public class Employee extends Model {
  // should be exists for update/delete
  @BindColumn(name = "id")
  public Long id;

  @BindColumn(name = "company_id")
  public Long companyId;

  @BindColumn(name = "name")
  public String name;

  @BindColumn(name = "age")
  public Integer age;
}

