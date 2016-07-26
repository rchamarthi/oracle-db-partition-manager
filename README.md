# Oracle Database partition manager - using PL/SQL

Check out the blog post [here](http://rchamarthi.com/2016/06/06/oracle-database-partition-manager-using-plsql/) for more details.

Add the necessary configuration based on 
*your partition naming standards
*Intervals
*Starting date 

Example : 

```
PARTITION_FREQUENCY : DAILY, MONTHLY
PARTITION_NAME_FORMAT : P{yyyy}{mm}{dd}, P{YYYY}{MM}
FIRST_PARTITION_NAME : P20100101
```

Run the package to create the missing partitions.

```
BEGIN
  pkg_partition_manager.p_create_missing_partitions (
    p_owner       => 'DWH',
    p_table_name  => 'SALES' -- daily partitoned table
  );
END;
/

ALTER TABLE DWH.SALES split partition PMAX at ( to_date('03-JAN-2010 00:00:00')) INTO          
(partition P20100102 ,partition PMAX) UPDATE global indexes;
ALTER TABLE DWH.SALES split partition PMAX at ( to_date('04-JAN-2010 00:00:00')) INTO          
(partition P20100103 ,partition PMAX) UPDATE global indexes;
ALTER TABLE DWH.SALES split partition PMAX at ( to_date('05-JAN-2010 00:00:00')) INTO
(partition P20100104 ,partition PMAX) UPDATE global indexes;
```
