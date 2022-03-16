drop table if exists companies;
create table if not exists companies (
  id int(11) not null auto_increment primary key,
  name varchar(64) not null,
  created_at timestamp default now()
);

drop table if exists employees;
create table if not exists employees (
  id int(11) not null auto_increment primary key,
  company_id int(11) null,
  name varchar(64) not null,
  age int(11) not null,
  created_at timestamp default now()
);

drop table if exists products;
create table if not exists products (
  id int(11) not null auto_increment primary key,
  company_id int(11) null,
  name varchar(64) not null,
  price int(11) not null,
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp,
  deleted_at timestamp null default null
);

drop table if exists users;
create table if not exists users (
    id int(11) not null auto_increment primary key,
    name varchar(64) not null,
    created_at timestamp default now()
);

drop table if exists docs;
create table if not exists docs (
    id int(11) not null auto_increment primary key,
    user_id int(11) not null,
    title varchar(64) not null,
    content varchar(1024) not null,
    props varchar(1024) null,
    created_at timestamp default now()
);

drop table if exists comments;
create table if not exists comments (
    id int(11) not null auto_increment primary key,
    user_id int(11) not null,
    doc_id int(11) not null,
    content varchar(1024) not null,
    created_at timestamp default now()
);
