#!/bin/bash
mysql --host=127.0.0.1 -P 3306 --user=root --password=testpw --loose-default-auth=mysql_native_password --database=vflow --execute="delete from samples where datetime < date_sub(NOW(), interval -2 hour);"
echo "old memsql rows has been cleared"
