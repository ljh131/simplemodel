create table if not exists companies (
  id int(11) not null auto_increment primary key,
  name varchar(64) not null,
  created_at timestamp default now()
);

create table if not exists employees (
  id int(11) not null auto_increment primary key,
  company_id int(11) null,
  name varchar(64) not null,
  age int(11) not null,
  created_at timestamp default now()
);

create table if not exists products (
  id int(11) not null auto_increment primary key,
  name varchar(64) not null,
  price int(11) not null,
  created_at timestamp default now(),
  deleted_at timestamp null default null
);
