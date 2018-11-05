-------**************T_ES_CLIENT_POSITION_TMP表**************-------

create table afaiv.TEMP1 as
select A.CLIENT_ID,
       B.fund_account,
       B.branch_no AS client_org_id,
       B.open_date AS open_date
      from client.t_client_counterclient A, client.t_client_fundaccount B
      WHERE A.CLIENT_ID=B.CLIENT_ID;
      
create table afaiv.TEMP2 as
select A.*,
      B.CLIENT_NAME,
      B.PHONECODE AS client_telephone,
      B.EMAIL AS client_email
      from afaiv.TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B
      WHERE A.CLIENT_ID=B.CLIENT_ID;
DROP TABLE afaiv.TEMP1;
      
create table afaiv.TEMP3 as
select A.*,
      B.organization_name AS client_org_name,
      B.organization_id AS main_serv_org_id,
      B.organization_name AS main_serv_org_name
      from afaiv.TEMP2 A, afaer.t_xtgl_organization B
      WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID;
DROP TABLE afaiv.TEMP2;

create table afaiv.TEMP4 as
select A.*,
      B.main_servuserid AS main_serv_id
      from afaiv.TEMP3 A, client.t_client_outclientid B
      WHERE A.CLIENT_ID=B.CLIENT_ID;
DROP TABLE afaiv.TEMP3;

create table afaiv.TEMP5 as
select A.*,
      B.login_id AS main_serv_hrid,
      B.user_name AS main_serv_name,
      B.phonecode AS main_serv_telephone
      from afaiv.TEMP4 A, afaer.t_xtgl_user B
      WHERE A.main_serv_id=B.USER_ID;
DROP TABLE afaiv.TEMP4;

create table afaiv.TEMP6 as
select A.*,
      B.VALID_CLIENT AS is_available
      from afaiv.TEMP5 A, CLIENT.T_INDEX_CLIENTCURENT B
      WHERE A.CLIENT_ID=B.CLIENT_ID;
DROP TABLE afaiv.TEMP5;

create table afaiv.TEMP7 as
select A.*,
      stock_code AS product_code
      from afaiv.TEMP6 A, CLIENT.t_hs06_stock B
      WHERE A.CLIENT_ID=B.CLIENT_ID;
DROP TABLE afaiv.TEMP6;

create table afaiv.TEMP8 as
select A.*,
      stock_name AS product_name
      from afaiv.TEMP7 A, quser.t_hs06_stkcode B
      WHERE A.product_code=B.STOCK_CODE;
DROP TABLE afaiv.TEMP7;

alter table afaiv.TEMP8           
add (
user_id_str            VARCHAR2(1000),
login_id_str        VARCHAR2(1000),
user_name_str        VARCHAR2(1000),
quota_1            NUMBER(19,2)  ,
quota_2            NUMBER(19,2)  ,
quota_3            NUMBER(19,2)  ,
quota_4            NUMBER(19,2)  ,
quota_5            NUMBER(19,2)  ,
quota_6            NUMBER(19,2)  ,
quota_7            NUMBER(19,2)  ,
quota_8            NUMBER(19,2)  ,
quota_9            NUMBER(19,2)  ,
quota_10          NUMBER(19,2)  ,
quota_name_1        VARCHAR2(30)  ,
quota_name_2        VARCHAR2(30)  ,
quota_name_3        VARCHAR2(30)  ,
quota_name_4        VARCHAR2(30)  ,
quota_name_5        VARCHAR2(30)  ,
quota_name_6        VARCHAR2(30)  ,
quota_name_7        VARCHAR2(30)  ,
quota_name_8        VARCHAR2(30)  ,
quota_name_9        VARCHAR2(30)  ,
quota_name_10        VARCHAR2(30)  ,
extend_value_1        VARCHAR2(30)  ,
extend_value_2        VARCHAR2(30)  ,
extend_value_3        VARCHAR2(30)  ,
extend_value_4        VARCHAR2(30)  ,
extend_value_5        VARCHAR2(30)  ,
extend_key_1        VARCHAR2(30)  ,
extend_key_2        VARCHAR2(30)  ,
extend_key_3        VARCHAR2(30)  ,
extend_key_4        VARCHAR2(30)  ,
extend_key_5        VARCHAR2(30)  ,
operate_type        VARCHAR2(10)  ,
batch_id          NUMBER        ,
elasticsearch_id      NUMBER        ,
elasticsearch_tag      VARCHAR2(10)  ,
elasticsearch_tag_name  VARCHAR2(300) ,
create_time        DATE          ,
update_time        DATE,
BATCH_SUB_ID	NUMBER,
EXTEND_VALUE_6	VARCHAR2(30),
EXTEND_KEY_6	VARCHAR2(30)         
     );   
     
CREATE TABLE afaiv.T_ES_CLIENT_POSITION_TMP AS 
SELECT * FROM afaiv.TEMP8;
DROP TABLE afaiv.TEMP8;
alter table afaiv.T_ES_CLIENT_POSITION_TMP modify  MAIN_SERV_ORG_ID null;


update afaiv.T_ES_CLIENT_POSITION_TMP t set t.client_email=(select EMAIL from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.is_available=(select t3.valid_client from CLIENT.T_INDEX_CLIENTCURENT t3 where t3.client_id=t.client_id),
                                            t.create_time=sysdate,
                                            t.update_time=sysdate,
                                            t.operate_type='1',
                                            t.ELASTICSEARCH_ID=ROWNUM,
                                            t.BATCH_SUB_ID=ceil(ROWNUM / 10000),
                                            t.batch_id=mod(t.batch_sub_id,8)+1,
                                            t.elasticsearch_tag='01', 
                                            t.elasticsearch_tag_name='二级持仓';
commit;

update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_hrid=(select u.hrid from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id  and u.user_id=d.user_id and rownum<=1);
commit;
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_name=(select u.user_name from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1);
commit;
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_telephone=(select u.phonecode from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1);
commit;
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_org_name=(select t4.organization_name from t_xtgl_organization t4 where t4.organization_id=t.main_serv_org_id and rownum<=1);
commit;
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_id=(select t5.main_servuserid from  client.t_client_outclientid t5 where t5.client_id = t.client_id and rownum<1);
commit;
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.main_serv_org_id=(select u.organization_id from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE t.client_id=d.client_id and u.user_id=d.user_id and rownum<=1);
commit;

CREATE TABLE t_tmp_client_Test AS 
SELECT d.client_id
      ,'|' || listagg(d.user_id, '|') within group(ORDER BY d.client_id) || '|' AS USER_ID_STR
      ,'|' || listagg(u.login_id, '|') within group(ORDER BY d.client_id) || '|' AS login_id_str
      ,'|' || listagg(u.user_name, '|') within group(ORDER BY d.client_id) || '|' AS user_name_str
  FROM afaer.t_serv_servrela d, afaer.t_xtgl_user u
 WHERE u.user_id = d.user_id
   AND d.user_relatype = '2'
   AND u.rec_status = '1'
 GROUP BY client_id;

create index idx_tmp_client_Test on t_tmp_client_Test(Client_Id);
create index idx_ES_CLIENT_POSITION_TMP on afaiv.T_ES_CLIENT_POSITION_TMP(Client_Id);

update afaiv.T_ES_CLIENT_POSITION_TMP t 
   set ( t.user_id_str,t.login_id_str, t.user_name_str) = (select t1.USER_ID_STR,t1.login_id_str, t1.user_name_str 
   from t_tmp_client_Test t1 where  t.client_id=t1.client_id and rownum<=1);
commit;

UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'ready', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_position_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_position_index', '1. Oracle procedure completed', SYSDATE);


-------**************afaiv.t_es_client_base_tmp表**************-------
create table TEMP1 as
       select A.CLIENT_ID,
       F.FUND_ACCOUNT, 
       F.branch_no AS client_org_id, 
       F.open_date,
       (SELECT o.organization_name
               FROM afaer.t_xtgl_organization o
               WHERE o.organization_id = F.branch_no
               AND o.rec_status = '1'
               AND rownum <= 1) AS client_org_name,
        '1' AS operate_type,
        'client_base_index' AS elasticsearch_tag
        ,'客户' AS elasticsearch_tag_name
        , SYSDATE AS create_time
        , SYSDATE AS update_time 
        from client.t_client_counterclient A, client.t_client_fundaccount F 
         WHERE A.CLIENT_ID=F.CLIENT_ID;
    
create table TEMP2 as
      select A.*,B.CLIENT_NAME,B.PHONECODE AS client_telephone,B.EMAIL AS client_email FROM TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B WHERE A.CLIENT_ID=B.CLIENT_ID;
      DROP TABLE TEMP1;

create table TEMP3 as
      select A.*, B.MAIN_SERVUSERID AS main_serv_id FROM TEMP2 A, client.t_client_outclientid B WHERE A.CLIENT_ID=B.CLIENT_ID;
      DROP TABLE TEMP2;

create table TEMP4 as
      select A.*, B.login_id AS main_serv_hrid , B.user_name AS main_serv_name, B.phonecode AS main_serv_telephone FROM TEMP3 A, afaer.t_xtgl_user B WHERE A.main_serv_id=B.USER_ID;
      DROP TABLE TEMP3;

create table TEMP5 as
      select A.*, B.organization_id AS main_serv_org_id, B.organization_name AS main_serv_org_name FROM TEMP4 A, afaer.t_xtgl_organization B WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID;
      DROP TABLE TEMP4;

alter table TEMP5           
      add (batch_id NUMBER,
      elasticsearch_id NUMBER);   

CREATE TABLE afaiv.t_es_client_base_tmp AS 
      SELECT * FROM TEMP5;
      DROP TABLE TEMP5;
alter table afaiv.t_es_client_base_tmp          
add (
      BATCH_SUB_ID	NUMBER,		
      IS_AVAILABLE	VARCHAR2(1));	

update afaiv.t_es_client_base_tmp t set
               t.BATCH_SUB_ID=ceil(ROWNUM / 10000);
update afaiv.t_es_client_base_tmp t set 
               t.batch_id=mod(t.batch_sub_id,8)+1,
               t.ELASTICSEARCH_ID=ROWNUM,
               t.is_available=(select t3.valid_client from CLIENT.T_INDEX_CLIENTCURENT t3 where t3.client_id=t.client_id and rownum<=1);
commit;
update afaiv.t_es_client_base_tmp t set t.main_serv_telephone=(select u.phonecode from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1);
commit;
update afaiv.t_es_client_base_tmp t set t.client_email=(select EMAIL from client.t_client_outclientid_info f where f.client_id=t.client_id);
commit


UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'ready', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_base_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_base_index', '1. Oracle procedure completed', SYSDATE);
commit;


-------**************AFAIV.T_ES_USER_MODULAR_TMP表**************-------

-- Create table
create table AFAIV.T_ES_USER_MODULAR_TMP
(
  USER_ID                NUMBER not null,
  LOGIN_ID               VARCHAR2(300) not null,
  MODULAR_ID             NUMBER not null,
  MODULAR_NAME           VARCHAR2(300) not null,
  MODULAR_NAME_ALL       VARCHAR2(1000) not null,
  MODULAR_URL            VARCHAR2(300),
  OPERATE_TYPE           VARCHAR2(10) not null,
  BATCH_ID               NUMBER,
  ELASTICSEARCH_ID       NUMBER,
  ELASTICSEARCH_TAG      VARCHAR2(20),
  ELASTICSEARCH_TAG_NAME VARCHAR2(300),
  CREATE_TIME            DATE not null,
  UPDATE_TIME            DATE not null,
  BATCH_SUB_ID           NUMBER
)
tablespace TBS_AFAIV_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 128
    next 8
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
-- Add comments to the table 
comment on table AFAIV.T_ES_USER_MODULAR_TMP
  is '用户菜单权限信息';
-- Add comments to the columns 
comment on column AFAIV.T_ES_USER_MODULAR_TMP.USER_ID
  is '员工编号';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.LOGIN_ID
  is '员工工号';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_ID
  is '菜单编号';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_NAME
  is '菜单名称';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_NAME_ALL
  is '菜单全路径名称（XX中心-XXXX-XXXX）';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_URL
  is '路径URL';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.OPERATE_TYPE
  is '1新增；2更新';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.BATCH_ID
  is '批次号';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_ID
  is 'ES编号';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_TAG
  is 'user_modular_index';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_TAG_NAME
  is '默认"菜单"';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.CREATE_TIME
  is '插入时间';
comment on column AFAIV.T_ES_USER_MODULAR_TMP.UPDATE_TIME
  is '更新时间';


insert into afaiv.T_ES_USER_MODULAR_TMP (USER_ID,LOGIN_ID,MODULAR_ID,  MODULAR_NAME, MODULAR_NAME_ALL, MODULAR_URL, OPERATE_TYPE, BATCH_ID, ELASTICSEARCH_TAG, ELASTICSEARCH_TAG_NAME, CREATE_TIME, UPDATE_TIME)
       select distinct a.user_id, a.login_id, d.modular_id, d.modular_name, 'tmp', d.url, '1', null, 'user_modular_index', '菜单', sysdate, sysdate
       from t_xtgl_user a, t_xtgl_userrole b, t_xtgl_rolemodular c, t_xtgl_modular d, dual
       where a.user_id=b.user_id and b.role_id=c.role_id and c.modular_id=d.modular_id and a.rec_status=1 and d.isvalid='1';


update afaiv.t_es_user_modular_tmp t set t.elasticsearch_id=ROWNUM;
update afaiv.t_es_user_modular_tmp t set  t.batch_sub_id=ceil(ROWNUM / 10000);
update afaiv.t_es_user_modular_tmp t set  t.batch_id=mod( t.batch_sub_id,8)+1;
UPDATE afaiv.t_es_user_modular_tmp t SET ELASTICSEARCH_ID=ROWNUM, BATCH_SUB_ID=ceil(ROWNUM / 10000);
commit;

BEGIN
FOR m IN (SELECT DISTINCT modular_id FROM afaer.t_xtgl_modular ) LOOP
UPDATE afaiv.t_es_user_modular_tmp t
  SET t.modular_name_all =
      (SELECT REPLACE(wm_concat(modular_name), ',', '-')
         FROM (SELECT d.modular_name
                 FROM t_xtgl_modular d
                START WITH d.modular_id = m.modular_id
               CONNECT BY PRIOR parent_id = d.modular_id
                ORDER BY LEVEL DESC))
WHERE t.modular_id = m.modular_id;
COMMIT;
END LOOP;
END;

-------**************afaiv.T_ES_USER_HELP_TMP表(4.898s)**************-------
CREATE TABLE afaiv.T_ES_USER_HELP_TMP(
USER_ID NUMBER(10),
LOGIN_ID VARCHAR2(50),
MODULAR_ID NUMBER(18), 
MODULAR_NAME VARCHAR2(200),
MODULAR_NAME_ALL VARCHAR2(1000), 
OPERATE_TYPE VARCHAR2(1), 
ELASTICSEARCH_ID NUMBER, 
BATCH_ID NUMBER, 
ELASTICSEARCH_TAG VARCHAR2(200), 
ELASTICSEARCH_TAG_NAME VARCHAR2(200), 
CREATE_TIME DATE, 
UPDATE_TIME DATE, 
BATCH_SUB_ID NUMBER,
FUNCTION_ID NUMBER, 
FUNCTION_NAME VARCHAR2(200), 
CHAPTER_ID NUMBER(18),
DIGEST VARCHAR2(600)
);

insert into afaiv.T_ES_USER_HELP_TMP (USER_ID,LOGIN_ID,MODULAR_ID ,FUNCTION_ID, FUNCTION_NAME, CHAPTER_ID,DIGEST)
select distinct a.user_id,
                a.login_id,
                g.modular_id,
                g.function_id,
                g.function_name,
                g.chapter_id,
                g.digest
  from t_xtgl_user a,
       t_xtgl_userrole b,
       t_xtgl_rolemodular c,
       (select d.chapter_id,
               d.modular_id,
               d.function_id,
               d.digest,
               (select e.modular_name
                  from t_xtgl_modular e
                 where e.modular_id = d.modular_id) modular_name,
               (select f.function_name
                  from t_xtgl_function f
                 where f.function_id = d.function_id) function_name
          from t_chapter_info d
         where d.status = 1
           and d.dle_status = 1) g
 where a.user_id = b.user_id
   and b.role_id = c.role_id
   and c.modular_id = g.modular_id;
   
update afaiv.T_ES_USER_HELP_TMP t set t.MODULAR_NAME=(select distinct MODULAR_NAME from t_xtgl_modular d where d.modular_id=t.modular_id),
                                      t.OPERATE_TYPE='1',
                                      t.ELASTICSEARCH_ID=ROWNUM,
                                      t.ELASTICSEARCH_TAG='user_modular_index', 
                                      t.ELASTICSEARCH_TAG_NAME='菜单', 
                                      t.CREATE_TIME=sysdate, 
                                      t.UPDATE_TIME=sysdate, 
                                      t.BATCH_SUB_ID=ceil(ROWNUM / 10000);

update afaiv.T_ES_USER_HELP_TMP t set t.batch_id=mod(t.batch_sub_id,8);
commit; 
BEGIN
       FOR m IN (SELECT DISTINCT modular_id FROM afaer.t_xtgl_modular ) LOOP
       UPDATE  afaiv.T_ES_USER_HELP_TMP t
       SET t.modular_name_all =
        (SELECT REPLACE(wm_concat(modular_name), ',', '-')
           FROM (SELECT d.modular_name
                   FROM t_xtgl_modular d
                  START WITH d.modular_id = m.modular_id
                 CONNECT BY PRIOR parent_id = d.modular_id
                  ORDER BY LEVEL DESC))
                  WHERE t.modular_id = m.modular_id;
       COMMIT;
       END LOOP;
END;
