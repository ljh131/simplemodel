package me.zerosquare.simplemodel;

import java.lang.annotation.*;
import java.lang.reflect.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BindColumn {
  String name();
}
