--存储过程的package
create or replace package PKG_PRO_HANDLE_AFADIV is

  -- Author  : XYZQ
  -- Created : 2018-10-26 13:35:54
  -- Purpose : 
                             
      PROCEDURE pro_esclientbasehandler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  );
  
        PROCEDURE pro_esclientpositionhandler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  );

end PKG_PRO_HANDLE_AFADIV;


--存储过程1body
create or replace procedure pro_esclientpositionhandler(param1 in varchar2, param2 in varchar2) is
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

UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'start', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_position_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_position_index', '1. Oracle procedure completed', SYSDATE);
end pro_esclientpositionhandler;

--存储过程2body
create or replace procedure pro_esclientbasehandler(param1 in varchar2, param2 in varchar2) is
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

