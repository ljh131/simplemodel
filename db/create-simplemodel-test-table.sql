--drop table companies;
--drop table employees;
--drop table products;

create table companies (
  id int(11) not null auto_increment primary key,
  name varchar(64) not null,
  created_at timestamp default now()
);

create table employees (
  id int(11) not null auto_increment primary key,
  company_id int(11) null,
  name varchar(64) not null,
  age int(11) not null,
  created_at timestamp default now()
);

create table products (
  id int(11) not null auto_increment primary key,
  name varchar(64) not null,
  price int(11) not null,
  created_at timestamp default now(),
  deleted_at timestamp null default null
);


