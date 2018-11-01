create or replace package body PKG_PRO_HANDLE_AFADIV is

   procedure pro_esclientbasehandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
     EXECUTE IMMEDIATE 'create table TEMP1 as
                               select A.CLIENT_ID,
                                      F.FUND_ACCOUNT, 
                                      F.branch_no AS client_org_id, 
                                      F.open_date,
                                      (SELECT o.organization_name
                                              FROM afaer.t_xtgl_organization o
                                              WHERE o.organization_id = F.branch_no
                                              AND o.rec_status = ''1''
                                              AND rownum <= 1) AS client_org_name,
                                       ''1'' AS operate_type,
                                       ''client_base_index'' AS elasticsearch_tag
                                       ,''客户'' AS elasticsearch_tag_name
                                       , SYSDATE AS create_time
                                       , SYSDATE AS update_time 
                                       from client.t_client_counterclient A, client.t_client_fundaccount F 
                                        WHERE A.CLIENT_ID=F.CLIENT_ID';
    
      EXECUTE IMMEDIATE 'create table TEMP2 as
      select A.*,B.CLIENT_NAME,B.PHONECODE AS client_telephone,B.EMAIL AS client_email FROM TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B WHERE A.CLIENT_ID=B.CLIENT_ID';
      EXECUTE IMMEDIATE 'DROP TABLE TEMP1';

      EXECUTE IMMEDIATE 'create table TEMP3 as
      select A.*, B.MAIN_SERVUSERID AS main_serv_id FROM TEMP2 A, client.t_client_outclientid B WHERE A.CLIENT_ID=B.CLIENT_ID';
      EXECUTE IMMEDIATE 'DROP TABLE TEMP2';

      EXECUTE IMMEDIATE 'create table TEMP4 as
      select A.*, B.login_id AS main_serv_hrid , B.user_name AS main_serv_name, B.phonecode AS main_serv_telephone FROM TEMP3 A, afaer.t_xtgl_user B WHERE A.main_serv_id=B.USER_ID';
      EXECUTE IMMEDIATE ' DROP TABLE TEMP3';

      EXECUTE IMMEDIATE 'create table TEMP5 as
      select A.*, B.organization_id AS main_serv_org_id, B.organization_name AS main_serv_org_name FROM TEMP4 A, afaer.t_xtgl_organization B WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID';
      EXECUTE IMMEDIATE 'DROP TABLE TEMP4';

      EXECUTE IMMEDIATE 'alter table TEMP5           
      add (batch_id NUMBER,
      elasticsearch_id NUMBER)';   
     
      EXECUTE IMMEDIATE 'CREATE TABLE afaiv.t_es_client_base_tmp AS 
      SELECT * FROM TEMP5';
      EXECUTE IMMEDIATE 'DROP TABLE TEMP5';
     
     UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'start', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_base_index' AND t.job_action = 'C';
     INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_base_index', '1. Oracle procedure completed', SYSDATE);
end pro_esclientbasehandler;

procedure pro_esclientpositionhandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
EXECUTE IMMEDIATE 'create table TEMP1 as
select A.CLIENT_ID,
       B.fund_account,
       B.branch_no AS client_org_id,
       B.open_date AS open_date
      from client.t_client_counterclient A, client.t_client_fundaccount B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
      
EXECUTE IMMEDIATE 'create table TEMP2 as
select A.*,
      B.CLIENT_NAME,
      B.PHONECODE AS client_telephone,
      B.EMAIL AS client_email
      from TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP1';
      
EXECUTE IMMEDIATE 'create table TEMP3 as
select A.*,
      B.organization_name AS client_org_name,
      B.organization_id AS main_serv_org_id,
      B.organization_name AS main_serv_org_name
      from TEMP2 A, afaer.t_xtgl_organization B
      WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP2';

EXECUTE IMMEDIATE 'create table TEMP4 as
select A.*,
      B.main_servuserid AS main_serv_id
      from TEMP3 A, client.t_client_outclientid B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP3';

EXECUTE IMMEDIATE 'create table TEMP5 as
select A.*,
      B.login_id AS main_serv_hrid,
      B.user_name AS main_serv_name,
      B.phonecode AS main_serv_telephone
      from TEMP4 A, afaer.t_xtgl_user B
      WHERE A.main_serv_id=B.USER_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP4';

EXECUTE IMMEDIATE 'create table TEMP6 as
select A.*,
      B.VALID_CLIENT AS is_available
      from TEMP5 A, CLIENT.T_INDEX_CLIENTCURENT B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP5';

EXECUTE IMMEDIATE 'create table TEMP7 as
select A.*,
      stock_code AS product_code
      from TEMP6 A, CLIENT.t_hs06_stock B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
EXECUTE IMMEDIATE 'DROP TABLE TEMP6';

EXECUTE IMMEDIATE 'create table TEMP8 as
select A.*,
      stock_name AS product_name
      from TEMP7 A, quser.t_hs06_stkcode B
      WHERE A.product_code=B.STOCK_CODE';
EXECUTE IMMEDIATE 'DROP TABLE TEMP7';

EXECUTE IMMEDIATE 'alter table TEMP8           
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
update_time        DATE          
     )';   
     
EXECUTE IMMEDIATE 'CREATE TABLE afaiv.T_ES_CLIENT_POSITION_TMP AS 
SELECT * FROM TEMP8';
EXECUTE IMMEDIATE 'DROP TABLE TEMP8';

update afaiv.T_ES_CLIENT_POSITION_TMP t set t.open_date=(select open_date from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.client_telephone=(select PHONECODE from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.client_email=(select EMAIL from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.is_available=(select t3.valid_client from CLIENT.T_INDEX_CLIENTCURENT t3 where t3.client_id=t.client_id),
                                            t.user_id_str=(SELECT '|'||replace(wm_concat(d.user_id),',','|')||'|' FROM afaer.t_serv_servrela d WHERE d.client_id = t.client_id),
                                            t.login_id_str=(SELECT '|'||replace(wm_concat(t4.login_id),',','|')||'|' FROM afaer.t_serv_servrela d, afaer.t_xtgl_user t4 WHERE d.client_id = t.client_id and d.user_id=t4.user_id),
                                            t.user_name_str=(SELECT '|'||replace(wm_concat(t4.user_name),',','|')||'|' FROM afaer.t_serv_servrela d, afaer.t_xtgl_user t4 WHERE d.client_id = t.client_id and d.user_id=t4.user_id);
                                            t.main_serv_hrid=(select u.hrid from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id  and u.user_id=d.user_id and rownum<=1),
                                            t.main_serv_name=(select u.user_name from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1),
                                            t.main_serv_telephone=(select u.phonecode from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1),
                                            t.main_serv_org_id=(select u.organization_id from afaer.t_xtgl_user u,afaer.t_serv_servrela d WHERE d.client_id = t.client_id and u.user_id=d.user_id and rownum<=1),
                                            t.main_serv_org_name=(select t4.organization_name from t_xtgl_organization t4 where t4.organization_id=t.main_serv_org_id and rownum<=1),
                                            t.main_serv_id=(select t5.main_servuserid from  client.t_client_outclientid t5 where t5.client_id = t.client_id and rownum<1);

UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'start', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_position_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_position_index', '1. Oracle procedure completed', SYSDATE);
end pro_esclientpositionhandler;


procedure pro_esusermodularhadler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
     insert into afaiv.T_ES_USER_MODULAR_TMP (USER_ID,LOGIN_ID,MODULAR_ID,  MODULAR_NAME, MODULAR_NAME_ALL, MODULAR_URL, OPERATE_TYPE, BATCH_ID, ELASTICSEARCH_TAG, ELASTICSEARCH_TAG_NAME, CREATE_TIME, UPDATE_TIME)
       select distinct a.user_id, a.login_id, d.modular_id, d.modular_name, 'tmp', d.url, '1', null, 'user_modular_index', '菜单', sysdate, sysdate
       from t_xtgl_user a, t_xtgl_userrole b, t_xtgl_rolemodular c, t_xtgl_modular d, dual

       where a.user_id=b.user_id and b.role_id=c.role_id and c.modular_id=d.modular_id;


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
end pro_esusermodularhadler;


procedure pro_esuserhelphandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
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
end pro_esuserhelphandler;





  
procedure PRO_HANDLE_AFADIV(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin

  pro_esclientbasehandler(o_code ,o_note);
  pro_esclientpositionhandler(o_code ,o_note);
  
end PRO_HANDLE_AFADIV;


end PKG_PRO_HANDLE_AFADIV;
