package me.zerosquare.simplemodel.model;

import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;

@Table(name = "docs")
public class Doc extends Model {
  @Column
  public Long id;

  @Column(name = "user_id")
  public Long userId;

  @Column
  public String title;

  @Column
  public String content;

  @Column
  public String props;

  public Doc() {
  }

  public Doc(Long userId, String title, String content) {
    this.userId = userId;
    this.title = title;
    this.content = content;
  }
}

