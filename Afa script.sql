--0、定时调度存储过程
DECLARE
  myjob NUMBER;
BEGIN
  dbms_job.submit(myjob, 'DECLARE
  o_code NUMBER;
  o_note VARCHAR2(200);
BEGIN
  pkg_sync_data.pro_sync_afa_data(o_code, o_note);
END;', SYSDATE, 'TRUNC(sysdate) + 1.175');
  COMMIT;
END;



SELECT * FROM user_jobs;

BEGIN
dbms_job.remove(482);
//dbms_job.break(482);
END;
/






---1、此表的数据量达到700多万条，在进行更新的时候，所耗时间巨大。因此改用索引更新，时间降到了1分钟左右-----
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

update afaiv.T_ES_CLIENT_POSITION_TMP t set t.client_email=(select EMAIL from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.is_available=(select t3.valid_client from CLIENT.T_INDEX_CLIENTCURENT t3 where t3.client_id=t.client_id),
                                            t.create_time=sysdate,
                                            t.update_time=sysdate,
                                            t.operate_type='1',
                                            t.ELASTICSEARCH_ID=ROWNUM,
                                            t.BATCH_SUB_ID=ceil(ROWNUM / 10000),
                                            t.batch_id=mod(t.batch_sub_id,8)+1;
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