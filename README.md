#### glue job

##### redshift connector whl
```
# redshift connector lib
wget https://files.pythonhosted.org/packages/5d/61/ac13afc2c4b04e67675584b7caa5e7fcc4168fce79839edba866f01c3c5f/redshift_connector-2.0.906-py3-none-any.whl
aws s3 cp redshift_connector-2.0.906-py3-none-any.whl s3://app-util/

wget https://files.pythonhosted.org/packages/2c/19/04f9b178c2d8a15b076c8b5140708fa6ffc5601fb6f1e975537072df5b2a/mergedeep-1.3.4-py3-none-any.whl
aws s3 cp mergedeep-1.3.4-py3-none-any.whl s3://app-util/
```
##### glue job parameters
```
#  需要添加的job参数
--region_name ap-southeast-1
--bucket app-util 
--redshift_secret_id redshift-producer 
--sql_file test.sql
--extra-py-files s3://app-util/redshift_connector-2.0.906-py3-none-any.whl,s3://app-util/mergedeep-1.3.4-py3-none-any.whl
# 动态日期参数,可以添加任意多个，所有日期的计算的基准日期为前一天的零点零分零秒,days,months,hours，minutes参数任意组合,下面是一些例子
--day1 days=-1,%Y-%m-%d     
--day2 months=-1,days=-1,%Y-%m-%d
--month1 months=-1,days=-1,hours=-1,%Y-%m-%d %H
--month2 months=0,days=-1,hours=-1,%Y-%m-%d %H
--hour1 hours=-1,%Y-%m-%d %H
--minute1 minutes=-1,%Y-%m-%d %H:%M   
...
# 例如 
create table test_ff1 as select 'day1' as col1, 'month1' as col2, 'month2' as col3, 'hour1' as col4, 'minute1' as col5;
被转换为
create table test_ff1 as select '2022-04-26' as col1, '2022-03-25 23' as col2, '2022-04-25 23' as col3, '2022-04-26 23' as col4, '2022-04-26 23:59' as col5
# glue job 运行时的网络，最佳实践是私有子网+NAT，glue的subnet和redshift要通，安全组配置
```

#### workflow properties
```
# 使用glue workflow调度作业时，可以为workflow下的所有job设置统一的参数
# 下面三个为字workflow设置的统一参数
region_name ap-southeast-1
bucket app-util 
redshift_secret_id redshift-producer 
# 动态日期参数,可以添加任意多个，所有日期的计算的基准日期为前一天的零点零分零秒,days,months,hours，minutes参数任意组合,下面是一些例子
day1 days=-1,%Y-%m-%d     
day2 months=-1,days=-1,%Y-%m-%d
month1 months=-1,days=-1,hours=-1,%Y-%m-%d %H
month2 months=0,days=-1,hours=-1,%Y-%m-%d %H
hour1 hours=-1,%Y-%m-%d %H
minute1 minutes=-1,%Y-%m-%d %H:%M   
...
# 例如 
create table test_ff1 as select 'day1' as col1, 'month1' as col2, 'month2' as col3, 'hour1' as col4, 'minute1' as col5;
被转换为
create table test_ff1 as select '2022-04-26' as col1, '2022-03-25 23' as col2, '2022-04-25 23' as col3, '2022-04-26 23' as col4, '2022-04-26 23:59' as col5

# job中为作业设置的参数, 也可以包含日期参数或者其他参数，如果和workflow重复，job参数优先级高
--sql_file test.sql
--extra-py-files s3://app-util/redshift_connector-2.0.906-py3-none-any.whl,s3://app-util/mergedeep-1.3.4-py3-none-any.whl

```

##### redshift conn
```
为了安全和方便，使用Secrets Manager管理redshift连接。需要在Secrets Manager创建秘钥，对于Redshift选择集群会自动生成相关秘钥值，
再添加一个database的秘钥key，值为要连接的database

```