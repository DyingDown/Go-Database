create table student (id int, name string);
insert into student (id, name) values (1, 'John Doe');
insert into student (id, name) values (2, 'Jane Doe');
insert into student (id, name) values (3, 'Jack Doe');
update student set name = 'Jane Smith' where id = 2;
select * from student where id = 2;
exit;

rm -rf /tmp/test && mkdir /tmp/test && go run main.go --server --create --path=/tmp/test/