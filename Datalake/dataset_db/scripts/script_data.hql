# CREACION DE BASES DE DATOS 
# ===================================================


CREATE DATABASE IF NOT EXISTS LANDING
COMMENT 'BASE DE DATOS LANDING'
LOCATION '/dl_databrothers/databases/landing'
WITH DBPROPERTIES ('creator' = 'Carlos Castillo', 'date' = '2020-10-25');

CREATE DATABASE IF NOT EXISTS operational
COMMENT 'BASE DE DATOS operational'
LOCATION '/dl_databrothers/databases/operational'
WITH DBPROPERTIES ('creator' = 'Carlos Castillo', 'date' = '2020-10-25');

CREATE DATABASE IF NOT EXISTS business
COMMENT 'BASE DE DATOS business'
LOCATION '/dl_databrothers/databases/business'
WITH DBPROPERTIES ('creator' = 'Carlos Castillo', 'date' = '2020-10-25');

# CREACION DE TABLAS EN BD LANDING 
# ===================================================


CREATE EXTERNAL TABLE LANDING.CLIENTE_TEXTFILE (
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ('creator' = 'Carlos Castillo', 'date' = '2020-10-25', 'skip.header.line.count'='1');


CREATE EXTERNAL TABLE LANDING.EMPRESA_TEXTFILE (
ID_EMPRESA STRING,
NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ('creator' = 'Carlos Castillo', 'date' = '2020-10-25', 'skip.header.line.count'='1');


CREATE EXTERNAL TABLE LANDING.TRANSACCION_TEXTFILE (
ID_CLIENTE STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ('creator' = 'Carlos Castillo', 'date' = '2020-10-25', 'skip.header.line.count'='1');


# CARGAR DATA EN BD LANDING 
# ===================================================


LoAD DATA LOCAL INPATH 'dataset_db/data/in/cliente.data' into table LANDING.CLIENTE_TEXTFILE;
LoAD DATA LOCAL INPATH 'dataset_db/data/in/empresa.data' into table LANDING.EMPRESA_TEXTFILE;
LoAD DATA LOCAL INPATH 'dataset_db/data/in/transacciones.data' into table LANDING.TRANSACCION_TEXTFILE;


MSCK REPAIR TABLE LANDING.CLIENTE_TEXTFILE;
MSCK REPAIR TABLE LANDING.EMPRESA_TEXTFILE;
MSCK REPAIR TABLE LANDING.TRANSACCION_TEXTFILE;


# VALIDACION DE DATA EN BD LANDING 
# ===================================================

select * from LANDING.CLIENTE_TEXTFILE limit 10;
select * from LANDING.EMPRESA_TEXTFILE limit 10;
select * from LANDING.TRANSACCION_TEXTFILE limit 10;


# CREACION DE TABLAS EN BD OPERATIONAL
# ===================================================


create EXTERNAL TABLE OPERATIONAL.CLIENTE_AVRO_SNAPPY
comment 'TABLA DE clientes formato AVRO'
STORED AS AVRO
LOCATION '/dl_databrothers/databases/operational/cliente_avro_snappy'
tblproperties ('avro.schema.url'='hdfs:///dl_databrothers/schema/operational/cliente_avro.avsc', 'avro.output.codec'='snappy');


create EXTERNAL TABLE OPERATIONAL.EMPRESA_AVRO_SNAPPY
comment 'TABLA DE empresa formato AVRO'
STORED AS AVRO
LOCATION '/dl_databrothers/databases/operational/empresa_avro_snappy'
tblproperties ('avro.schema.url'='hdfs:///dl_databrothers/schema/operational/empresa_avro.avsc', 'avro.output.codec'='snappy');


create EXTERNAL TABLE OPERATIONAL.TRANSACCION_PARQUET_SNAPPY (
ID_CLIENTE STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
comment 'TABLA DE transacciones formato parquet snappy'
STORED as PARQUET
tblproperties ('creator' = 'Carlos Castillo', 'date' = '2020-10-25', 'parquet.compression'='SNAPPY');


# CARGA DATA EN TABLAS -- DB OPERATIONAL 
# ===================================================

set hive.exec.compress.output= true;
set avro.output.codec=snappy;

insert overwrite TABLE OPERATIONAL.CLIENTE_AVRO_SNAPPY
select * from LANDING.CLIENTE_TEXTFILE
where id != 'ID_CLIENTE';


set hive.exec.compress.output= true;
set avro.output.codec=snappy;

insert overwrite TABLE OPERATIONAL.EMPRESA_AVRO_SNAPPY
select * from LANDING.EMPRESA_TEXTFILE
where id_empresa != 'ID_EMPRESA';


set hive.exec.compress.output= true;
set parquet.compression=SNAPPY;

insert overwrite TABLE OPERATIONAL.TRANSACCION_PARQUET_SNAPPY  
select * from LANDING.TRANSACCION_TEXTFILE
where id_cliente != 'ID_CLIENTE';


# VALIDACION DE TABLAS CARGADAS -- DB OPERATIONAL 
# ===================================================

select * from OPERATIONAL.CLIENTE_AVRO_SNAPPY limit 10;
select * from OPERATIONAL.EMPRESA_AVRO_SNAPPY limit 10;
select * from OPERATIONAL.TRANSACCION_PARQUET_SNAPPY limit 10;


# CREACION DE TABLAS EN BD BUSINESS
# ===================================================

create EXTERNAL TABLE BUSINESS.HISTORICA_TRANSACCION (
ID_CLIENTE STRING,
NOMBRE_CLIENTE STRING,
EDAD_CLIENTE STRING,
SALARIO_CLIENTE STRING,
ID_EMPRESA STRING,
NOMBRE_EMPRESA STRING,
MONTO_TRANSACCION DOUBLE
)
comment 'TABLA historica DE transacciones formato parquet snappy particionada'
PARTITIONED BY (FECHA STRING)
STORED as PARQUET
tblproperties ('creator' = 'Carlos Castillo', 'date' = '2020-10-25', 'parquet.compression'='SNAPPY');


# CARGA DATA EN TABLAS -- DB BUSINESS
# ===================================================

set hive.exec.compress.output= true;
set parquet.compression=SNAPPY;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;


insert overwrite TABLE BUSINESS.HISTORICA_TRANSACCION partition (fecha)
select  TRX.id_cliente, cli.nombre as nombre_cliente, cli.edad as edad_cliente, 
cli.salario as salario_cliente,trx.id_empresa, emp.nombre as nombre_empresa, 
sum(trx.monto) as monto_transaccion,
trx.fecha
from OPERATIONAL.TRANSACCION_PARQUET_SNAPPY TRX
inner join OPERATIONAL.CLIENTE_AVRO_SNAPPY CLI on TRX.ID_CLIENTE = CLI.ID_CLIENTE
inner join OPERATIONAL.EMPRESA_AVRO_SNAPPY EMP on TRX.ID_EMPRESA = EMP.ID_EMPRESA
group by TRx.id_cliente, cli.nombre , cli.edad, cli.salario, trx.id_empresa, emp.nombre, trx.fecha;


# VALIDACION DE TABLA -- DB BUSINESS 
# ===================================================

select * from BUSINESS.HISTORICA_TRANSACCION limit 10;


# EXPORTANDO A ARCHIVO PLANO 
# ===================================================

set hive.cli.print.header=true;

INSERT OVERWRITE LOCAL DIRECTORY '/home/carlos/dataset_db/data/out'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select * from BUSINESS.HISTORICA_TRANSACCION;






