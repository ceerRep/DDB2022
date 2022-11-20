create table p1 (id int key, name char (100), nation char(3));
create table b1 (id int key, title char(100), authors char(200), publisher_id int, copies int);
create table c1 (id int key, name char (25));
create table o1 (customer_id int, book_id int, quantity int);

insert into p1 values (103000, 'p1', 'PRC');
insert into b1 values (204000, 'b1', 'a1', 103000, 5);
insert into c1 values (114514, 'cname');
