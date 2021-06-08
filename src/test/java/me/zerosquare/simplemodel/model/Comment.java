package me.zerosquare.simplemodel.model;

import me.zerosquare.simplemodel.Model;
import me.zerosquare.simplemodel.annotations.Column;
import me.zerosquare.simplemodel.annotations.Table;

@Table(name = "comments")
public class Comment extends Model {
  @Column
  public Long id;

  @Column(name = "user_id")
  public Long userId;

  @Column(name = "doc_id")
  public Long docId;

  @Column
  public String content;

  public Comment() {
  }

  public Comment(Long userId, Long docId, String comment) {
      this.userId = userId;
      this.docId = docId;
      this.content = comment;
  }
}
