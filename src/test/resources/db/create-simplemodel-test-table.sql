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
  name varchar(64) not null,
  price int(11) not null,
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp,
  deleted_at timestamp null default null
);