create table partition_metadata(
OWNER                  VARCHAR2(30) not null,
TABLE_NAME             VARCHAR2(30) not null,
PARTITION_FREQUENCY    VARCHAR2(30) not null,
PARTITION_NAME_FORMAT  VARCHAR2(30) not null,
PARTITION_TS_FORMAT    VARCHAR2(30) not null,
FIRST_PARTITION_NAME   VARCHAR2(30) not null,
MAXVAL_PARTITION_NAME  VARCHAR2(30) not null,
MAXVAL_TS_NAME_FORMAT  VARCHAR2(30) not null,
NUM_ADVANCE_PARTITIONS NUMBER       default 1 not null,       
ACTIVE_RECORD_FLAG     VARCHAR2(1)  default 'Y' not null,
DW_UPDATED_BY          VARCHAR2(30) not null,
DW_LAST_UPDATED        DATE         not null
);

