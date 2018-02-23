package me.zerosquare.simplemodel;

import java.lang.annotation.*;
import java.lang.reflect.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface BindTable {
  String name();
}

