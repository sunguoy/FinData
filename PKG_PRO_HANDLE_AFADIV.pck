create or replace package body PKG_PRO_HANDLE_AFADIV is

   procedure pro_esclientbasehandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
execute immediate'create table esclientbasehandler_TEMP1 as
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
    
execute immediate'create table esclientbasehandler_TEMP2 as
      select A.*,B.CLIENT_NAME,B.PHONECODE AS client_telephone,B.EMAIL AS client_email FROM TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'TRUNCATE TABLE esclientbasehandler_TEMP1';

execute immediate'create table esclientbasehandler_TEMP3 as
      select A.*, B.MAIN_SERVUSERID AS main_serv_id FROM TEMP2 A, client.t_client_outclientid B WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'TRUNCATE TABLE esclientbasehandler_TEMP2';

execute immediate'create table esclientbasehandler_TEMP4 as
      select A.*, B.login_id AS main_serv_hrid , B.user_name AS main_serv_name, B.phonecode AS main_serv_telephone FROM TEMP3 A, afaer.t_xtgl_user B WHERE A.main_serv_id=B.USER_ID';
execute immediate'TRUNCATE TABLE esclientbasehandler_TEMP3';

execute immediate'create table esclientbasehandler_TEMP5 as
      select A.*, B.organization_id AS main_serv_org_id, B.organization_name AS main_serv_org_name FROM TEMP4 A, afaer.t_xtgl_organization B WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID';
execute immediate'TRUNCATE TABLE esclientbasehandler_TEMP4';

execute immediate'alter table esclientbasehandler_TEMP5           
      add (batch_id NUMBER,
      elasticsearch_id NUMBER)';   

execute immediate'CREATE TABLE afaiv.t_es_client_base_tmp AS 
      SELECT * FROM TEMP5';
execute immediate'TRUNCATE TABLE esclientbasehandler_TEMP5';
execute immediate'alter table afaiv.t_es_client_base_tmp          
add (
      BATCH_SUB_ID  NUMBER,    
      IS_AVAILABLE  VARCHAR2(1))';  

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
commit;

UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'ready', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_base_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_base_index', '1. Oracle procedure completed', SYSDATE);
commit;

end pro_esclientbasehandler;

procedure pro_esclientpositionhandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
execute immediate'create table afaiv.TEMP1 as
select A.CLIENT_ID,
       B.fund_account,
       B.branch_no AS client_org_id,
       B.open_date AS open_date
      from client.t_client_counterclient A, client.t_client_fundaccount B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
      
execute immediate'create table afaiv.TEMP2 as
select A.*,
      B.CLIENT_NAME,
      B.PHONECODE AS client_telephone,
      B.EMAIL AS client_email
      from afaiv.TEMP1 A, client.T_CLIENT_OUTCLIENTID_INFO B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'DROP TABLE afaiv.TEMP1';
      
execute immediate'create table afaiv.TEMP3 as
select A.*,
      B.organization_name AS client_org_name,
      B.organization_id AS main_serv_org_id,
      B.organization_name AS main_serv_org_name
      from afaiv.TEMP2 A, afaer.t_xtgl_organization B
      WHERE A.CLIENT_ORG_ID=B.ORGANIZATION_ID';
execute immediate'DROP TABLE afaiv.TEMP2';

execute immediate'create table afaiv.TEMP4 as
select A.*,
      B.main_servuserid AS main_serv_id
      from afaiv.TEMP3 A, client.t_client_outclientid B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'DROP TABLE afaiv.TEMP3';

execute immediate'create table afaiv.TEMP5 as
select A.*,
      B.login_id AS main_serv_hrid,
      B.user_name AS main_serv_name,
      B.phonecode AS main_serv_telephone
      from afaiv.TEMP4 A, afaer.t_xtgl_user B
      WHERE A.main_serv_id=B.USER_ID';
execute immediate'DROP TABLE afaiv.TEMP4';

execute immediate'create table afaiv.TEMP6 as
select A.*,
      B.VALID_CLIENT AS is_available
      from afaiv.TEMP5 A, CLIENT.T_INDEX_CLIENTCURENT B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'DROP TABLE afaiv.TEMP5';

execute immediate'create table afaiv.TEMP7 as
select A.*,
      stock_code AS product_code
      from afaiv.TEMP6 A, CLIENT.t_hs06_stock B
      WHERE A.CLIENT_ID=B.CLIENT_ID';
execute immediate'DROP TABLE afaiv.TEMP6';

execute immediate'create table afaiv.TEMP8 as
select A.*,
      stock_name AS product_name
      from afaiv.TEMP7 A, quser.t_hs06_stkcode B
      WHERE A.product_code=B.STOCK_CODE';
execute immediate'DROP TABLE afaiv.TEMP7';

execute immediate'alter table afaiv.TEMP8           
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
BATCH_SUB_ID  NUMBER,
EXTEND_VALUE_6  VARCHAR2(30),
EXTEND_KEY_6  VARCHAR2(30),
ECIF_ID  NUMBER(18) null         
     )';   
     
execute immediate'CREATE TABLE afaiv.T_ES_CLIENT_POSITION_TMP AS 
SELECT * FROM afaiv.TEMP8';
execute immediate'DROP TABLE afaiv.TEMP8';
execute immediate'alter table afaiv.T_ES_CLIENT_POSITION_TMP modify  MAIN_SERV_ORG_ID null';


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


-- 融资融券持仓 
insert into afaiv.t_es_client_position_tmp
  select a.client_id,
         a.fund_account as fundAccount,
         a.branch_no,
         '' as openDate,
         '' as clientName,
         '' as clientTelephone,
         '' as clientEmail,
         (select org.organization_name
            from t_xtgl_organization org
           where a.branch_no = org.organization_id) as 开户营业部,
         '' as mainServOrgId,
         '' as mainServOrgName,
         '' as mainServId,
         '' as mainServHrid,
         '' as mainServName,
         '' as mainServTelephone,
         '' as isAvailable,
         trim(a.stock_code) as "stockCode",
         b.STOCK_NAME as "stockName",
         '' as userIdStr,
         '' as loginIdStr,
         '' as userNameStr,
         to_char(a.cost_price, '99999999999990.99') as "costPrice", ---成本价 
         to_char(a.current_amount, '99999999999990.99') as "currentAmount", --股票余额
         to_char(a.current_amount + a.unfrozen_amount - a.frozen_amount,
                 '99999999999990.99') kyAmount, --可用余额=股票余额+解冻数量-冻结数量 
         to_char(b.LAST_PRICE, '99999999999990.99') as "assetPrice", --市价 
         to_char(b.LAST_PRICE * a.current_amount, '99999999999990.99') as "currentBalance", --当前市值=股票余额*市价 
         cast(((a.current_amount + a.correct_amount) * b.last_price -
              a.sum_buy_balance + a.sum_sell_balance) as number(20, 2)) as "ykBalance", --盈亏额
         CASE a.sum_buy_balance
           WHEN 0 THEN
            0
           ELSE
            cast(((((a.current_amount + a.correct_amount) * b.last_price -
                 a.sum_buy_balance + a.sum_sell_balance) /
                 a.sum_buy_balance)) * 100 as number(20, 2))
         END as "ykRate", --盈亏率=盈亏额/累计买入金额
         '' as quota8,
         '' as quota9,
         '' as quota10,
         '成本价(元)' as quotaName1,
         '股票余额' as quotaName2,
         '可用余额' as quotaName3,
         '盈亏率' as quotaName4,
         '基金市值(元)' as quotaName5,
         '盈亏额(元)' as quotaName6,
         '基金可用份额' as quotaName7,
         '' as quotaName8,
         '' as quotaName9,
         '' as quotaName10,
         a.stock_account as extendValue1, --股东账户
         a.exchange_type as extendValue2, --交易类别 
         a.fund_account as extendValue3,
         '' as extendValue4,
         '' as extendValue5,
         '股东账户' as extendKey1,
         '交易类别' as extendKey2,
         '信用账号' as extendKey3,
         '' as extendKey4,
         '' as extendKey5,
         '1' as operateType,
         '' as batchId,
         rownum as elasticsearchId,
         '04' as elasticsearchTag,
         '融资融券持仓' as elasticsearchTagName,
         sysdate,
         sysdate,
         '' as batchSubId,
         '',
         '',
         ''
    from client.T_FINA_STOCK a, quser.t_hs06_stkcode b
    where trim(a.stock_code) = b.stock_code(+)
     and a.exchange_type = b.exchange_type(+);
commit;


-- 场外开基
insert into afaiv.t_es_client_position_tmp
  select aa.client_id,
         aa.fund_account,
         aa.branch_no,
         '' as openDate,
         '' as clientName,
         '' as clientTelephone,
         '' as clientEmail,
         (select org.organization_name
            from t_xtgl_organization org
           where aa.BRANCH_NO = org.organization_id) as 开户营业部,
         '' as mainServOrgId,
         '' as mainServOrgName,
         '' as mainServId,
         '' as mainServHrid,
         '' as mainServName,
         '' as mainServTelephone,
         '' as isAvailable,
         to_char(aa.fund_code) as 基金代码,
         (select c.fund_name
            from quser.t_hs06_ofstkcode c
           where aa.fund_code = c.fund_code) as 基金名称,
         '' as userIdStr,
         '' as loginIdStr,
         '' as userNameStr,
         aa.jjfe as 基金份额,
         aa.jrjz as "最新净值(元)",
         cast(nvl(aa.yke, 0) as number(20, 2)) as "盈亏额(元)",
         cast(decode(aa.buy_money,
                     0,
                     0,
                     (nvl(aa.yke, 0) / aa.buy_money) * 100) as
              number(20, 2)) as "盈亏率",
         aa.jjsz as "基金市值(元)",
         aa.jjkyfe as 基金可用份额,
         '' as quota7,
         '' as quota8,
         '' as quota9,
         '' as quota10,
         '基金份额' as quotaName1,
         '最新净值(元)' as quotaName2,
         '盈亏额(元)' as quotaName3,
         '盈亏率' as quotaName4,
         '基金市值(元)' as quotaName5,
         '基金可用份额' as quotaName6,
         '' as quotaName7,
         '' as quotaName8,
         '' as quotaName9,
         '' as quotaName10,
         (select d.fund_name
            from quser.t_fund_company d
           where aa.fund_company = d.fund_company) as 基金公司,
         (select w.display_value
            from v_dict_business w
           where w.column_name = 'FUND_TYPE'
             and aa.FUND_TYPE = w.value) as 基金类型,
         aa.prodrisk_level as "产品风险等级",
         '' as extendValue4,
         '' as extendValue5,
         '基金公司' as extendKey1,
         '基金类型' as extendKey2,
         '产品风险等级' as extendKey3,
         '' as extendKey4,
         '' as extendKey5,
         '1' as operateType,
         '' as batchId,
         rownum as elasticsearchId,
         '02' as elasticsearchTag,
         '场外开基' as elasticsearchTagName,
         sysdate,
         sysdate,
         '' as batchSubId,
         '',
         '',
         NULL
    from (select a.fund_account,
                 a.client_id,
                 a.fund_code,
                 fi.stock_type fund_type,
                 a.branch_no,
                 a.fund_company,
                 sum((nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) *
                     a.cost_price) as buy_money,
                 sum((nvl(f.nav, 0) - nvl(a.cost_price, 0)) *
                     (nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0))) as yke,
                 sum(nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) jjfe,
                 sum(nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0) -
                     nvl(a.Frozen_Share, 0)) jjkyfe,
                 nvl(f.nav, 0) jrjz,
                 sum((nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) *
                     nvl(f.nav, 0)) as jjsz,
                 max(rl.DISPLAY_VALUE) prodrisk_level
            FROM (SELECT A.FUND_ACCOUNT FUND_ACCOUNT,
                         A.CLIENT_ID CLIENT_ID,
                         A.SECUM_ACCOUNT STOCK_ACCOUNT,
                         A.BRANCH_NO BRANCH_NO,
                         A.PROD_CODE FUND_CODE,
                         A.BUY_DATE BUY_DATE,
                         A.NET_NO NET_NO,
                         A.MONEY_TYPE MONEY_TYPE,
                         A.ALLOT_NO ALLOTNO,
                         A.BEGIN_AMOUNT BEGIN_SHARE,
                         A.CURRENT_AMOUNT CURRENT_SHARE,
                         A.DIVIDEND_WAY AUTO_BUY,
                         A.CHARGE_TYPE CHARGE_TYPE,
                         A.SECUM_MARKET_VALUE MARKET_VALUE,
                         A.TRANS_ACCOUNT TRANS_ACCOUNT,
                         NULL BACK_SHARE,
                         NULL BUSINESS_FROZEN_SHARE,
                         NULL CORRECT_SHARE,
                         A.PROD_COST_PRICE COST_PRICE,
                         NULL FROZEN_SHARE,
                         A.PROD_TA_NO FUND_COMPANY,
                         NULL IBRANCH_NO,
                         A.OC_DATE INIT_DATE,
                         NULL LONG_FROZEN_SHARE,
                         NULL POSITION_STR,
                         NULL STOCK_STATUS,
                         NULL STOCK_TYPE,
                         NULL TA_NO,
                         A.OC_DATE OC_DATE,
                         NVL(A.SUM_BUY_BALANCE, 0) SUM_BUY_BALANCE,
                         NVL(A.SUM_SELL_BALANCE, 0) SUM_SELL_BALANCE
                    FROM CLIENT.T_BANK_SECUMSHARE A
                   WHERE EXISTS (SELECT 1
                            FROM CLIENT.t_bank_prodcode PC
                           WHERE A.PROD_TA_NO = PC.PRODTA_NO
                             AND PC.PROD_TYPE = '5'
                             AND PC.PRODCODE_KIND = '1')) a,
                 client.t_client_outclientid ll,
                 quser.t_hs06_ofstkcode fi,
                 client.t_client_formalclient fc,
                 (SELECT SUBSTR(S.PRODTA_NO, 1, 4) AS FUND_COMPANY,
                         SUBSTR(S.PROD_CODE, 1, 6) AS FUND_CODE,
                         S.NET_VALUE AS NAV
                    FROM CLIENT.T_BANK_PRODPRICE S
                   WHERE EXISTS
                   (SELECT 1
                            FROM CLIENT.t_bank_prodcode PC
                           WHERE S.PRODTA_NO = PC.PRODTA_NO
                             AND PC.PROD_TYPE = '5'
                             AND PC.PRODCODE_KIND = '1')
                     AND S.OC_DATE =
                         (SELECT MAX(OC_DATE) FROM CLIENT.T_BANK_PRODPRICE)) f,
                 (select prod_code, prodrisk_level, DISPLAY_VALUE
                    from CLIENT.T_HS08_REALPRODCODE rr,
                         afaer.v_dict_business      bb
                   where column_name = 'PRODRISK_LEVEL'
                     and bb.value = prodrisk_level) rl
           where 1 = 1
             and ll.client_id = a.client_id
             and fc.ecif_id = ll.ecif_id
             and a.fund_code = fi.fund_code
             and a.fund_code = rl.prod_code(+)
             and a.fund_company = f.fund_company(+)
             and a.fund_code = f.fund_code(+)
             AND A.FUND_CODE NOT IN
                 (SELECT PRODUCT_CODE FROM AFAER.V_PROD_SETFINANCIAL)
           group by a.branch_no,
                    a.fund_code,
                    a.fund_account,
                    a.client_id,
                    a.fund_company,
                    f.nav,
                    fi.stock_type) aa;
commit;


-- 兴业资管  
insert into afaiv.t_es_client_position_tmp
  select aa.client_id,
         aa.fund_account,
         aa.branch_no,
         '' as openDate,
         '' as clientName,
         '' as clientTelephone,
         '' as clientEmail,
         (select org.organization_name
            from t_xtgl_organization org
           where aa.BRANCH_NO = org.organization_id) as 开户营业部,
         '' as mainServOrgId,
         '' as mainServOrgName,
         '' as mainServId,
         '' as mainServHrid,
         '' as mainServName,
         '' as mainServTelephone,
         '' as isAvailable,
         to_char(aa.fund_code) as 基金代码,
         (select c.fund_name
            from quser.t_hs06_ofstkcode c
           where aa.fund_code = c.fund_code) as 基金名称,
         '' as userIdStr,
         '' as loginIdStr,
         '' as userNameStr,
         aa.jjfe as 基金份额,
         aa.jrjz as "最新净值(元)",
         cast(nvl(aa.yke, 0) as number(20, 2)) as "盈亏额(元)",
         cast(decode(aa.buy_money,
                     0,
                     0,
                     (nvl(aa.yke, 0) / aa.buy_money) * 100) as
              number(20, 2)) as "盈亏率",
         aa.jjsz as "基金市值(元)",
         aa.jjkyfe as 基金可用份额,
         '' as quota7,
         '' as quota8,
         '' as quota9,
         '' as quota10,
         '基金份额' as quotaName1,
         '最新净值(元)' as quotaName2,
         '盈亏额(元)' as quotaName3,
         '盈亏率' as quotaName4,
         '基金市值(元)' as quotaName5,
         '基金可用份额' as quotaName6,
         '' as quotaName7,
         '' as quotaName8,
         '' as quotaName9,
         '' as quotaName10,
         (select d.fund_name
            from quser.t_fund_company d
           where aa.fund_company = d.fund_company) as 基金公司,
         (select w.display_value
            from v_dict_business w
           where w.column_name = 'FUND_TYPE'
             and aa.FUND_TYPE = w.value) as 基金类型,
         aa.prodrisk_level as "产品风险等级",
         '' as extendValue4,
         '' as extendValue5,
         '基金公司' as extendKey1,
         '基金类型' as extendKey2,
         '产品风险等级' as extendKey3,
         '' as extendKey4,
         '' as extendKey5,
         '1' as operateType,
         '' as batchId,
         rownum as elasticsearchId,
         '03' as elasticsearchTag,
         '兴业资管' as elasticsearchTagName,
         sysdate,
         sysdate,
         '' as batchSubId,
         '',
         '',
         NULL
    from (select a.fund_account,
                 a.client_id,
                 a.fund_code,
                 fi.stock_type fund_type,
                 a.branch_no,
                 a.fund_company,
                 sum((nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) *
                     a.cost_price) as buy_money,
                 sum((nvl(f.nav, 0) - nvl(a.cost_price, 0)) *
                     (nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0))) as yke,
                 sum(nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) jjfe,
                 sum(nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0) -
                     nvl(a.Frozen_Share, 0)) jjkyfe,
                 nvl(f.nav, 0) jrjz,
                 sum((nvl(a.CURRENT_SHARE, 0) - nvl(a.BACK_SHARE, 0)) *
                     nvl(f.nav, 0)) as jjsz,
                 max(rl.DISPLAY_VALUE) prodrisk_level
            FROM (SELECT A.FUND_ACCOUNT FUND_ACCOUNT,
                         A.CLIENT_ID CLIENT_ID,
                         A.SECUM_ACCOUNT STOCK_ACCOUNT,
                         A.BRANCH_NO BRANCH_NO,
                         A.PROD_CODE FUND_CODE,
                         A.BUY_DATE BUY_DATE,
                         A.NET_NO NET_NO,
                         A.MONEY_TYPE MONEY_TYPE,
                         A.ALLOT_NO ALLOTNO,
                         A.BEGIN_AMOUNT BEGIN_SHARE,
                         A.CURRENT_AMOUNT CURRENT_SHARE,
                         A.DIVIDEND_WAY AUTO_BUY,
                         A.CHARGE_TYPE CHARGE_TYPE,
                         A.SECUM_MARKET_VALUE MARKET_VALUE,
                         A.TRANS_ACCOUNT TRANS_ACCOUNT,
                         NULL BACK_SHARE,
                         NULL BUSINESS_FROZEN_SHARE,
                         NULL CORRECT_SHARE,
                         A.PROD_COST_PRICE COST_PRICE,
                         NULL FROZEN_SHARE,
                         A.PROD_TA_NO FUND_COMPANY,
                         NULL IBRANCH_NO,
                         A.OC_DATE INIT_DATE,
                         NULL LONG_FROZEN_SHARE,
                         NULL POSITION_STR,
                         NULL STOCK_STATUS,
                         NULL STOCK_TYPE,
                         NULL TA_NO,
                         A.OC_DATE OC_DATE,
                         NVL(A.SUM_BUY_BALANCE, 0) SUM_BUY_BALANCE,
                         NVL(A.SUM_SELL_BALANCE, 0) SUM_SELL_BALANCE
                    FROM CLIENT.T_BANK_SECUMSHARE A
                   WHERE EXISTS (SELECT 1
                            FROM CLIENT.t_bank_prodcode PC
                           WHERE A.PROD_TA_NO = PC.PRODTA_NO
                             AND PC.PROD_TYPE = '5'
                             AND PC.PRODCODE_KIND = '1')) a,
                 client.t_client_outclientid ll,
                 quser.t_hs06_ofstkcode fi,
                 client.t_client_formalclient fc,
                 (SELECT SUBSTR(S.PRODTA_NO, 1, 4) AS FUND_COMPANY,
                         SUBSTR(S.PROD_CODE, 1, 6) AS FUND_CODE,
                         S.NET_VALUE AS NAV
                    FROM CLIENT.T_BANK_PRODPRICE S
                   WHERE EXISTS
                   (SELECT 1
                            FROM CLIENT.t_bank_prodcode PC
                           WHERE S.PRODTA_NO = PC.PRODTA_NO
                             AND PC.PROD_TYPE = '5'
                             AND PC.PRODCODE_KIND = '1')
                     AND S.OC_DATE =
                         (SELECT MAX(OC_DATE) FROM CLIENT.T_BANK_PRODPRICE)) f,
                 (select prod_code, prodrisk_level, DISPLAY_VALUE
                    from CLIENT.T_HS08_REALPRODCODE rr,
                         afaer.v_dict_business      bb
                   where column_name = 'PRODRISK_LEVEL'
                     and bb.value = prodrisk_level) rl
           where 1 = 1
             and ll.client_id = a.client_id
             and fc.ecif_id = ll.ecif_id
             and a.fund_code = fi.fund_code
             and a.fund_code = rl.prod_code(+)
             and a.fund_company = f.fund_company(+)
             and a.fund_code = f.fund_code(+)
             AND a.fund_code in
                 (select product_code from afaer.v_prod_setfinancial)
           group by a.branch_no,
                    a.fund_code,
                    a.fund_account,
                    a.client_id,
                    a.fund_company,
                    f.nav,
                    fi.stock_type) aa;
commit;
                    
                    
-- 银行理财
insert into afaiv.t_es_client_position_tmp
select
  aa.client_id,
  aa.fund_account       as 资金账号,
  aa.ORGANIZATION_ID,
  '' as openDate,
  aa.client_name   as 客户姓名,
  '' as clientTelephone,
       '' as clientEmail,      
  (select org.organization_name from t_xtgl_organization org where aa.ORGANIZATION_ID =org.organization_id )  as 开户营业部,
  '' as mainServOrgId,
       '' as mainServOrgName,
       '' as mainServId,
       '' as mainServHrid,
       '' as mainServName,
       '' as mainServTelephone,
       '' as isAvailable,
  PRODCODE           产品代码 ,
  PRODNAME          产品名称  ,
  '' as userIdStr,
  '' as loginIdStr,
  '' as userNameStr,  
  CURRENTAMOUNT     当前数量  ,
  FROZENAMOUNT     冻结数量  ,
  PRODPRERATIO 预期年收益率 ,
   '' as quota4,
    '' as quota5,
     '' as quota6,
      '' as quota7, 
      '' as quota8,
      '' as quota9, 
      '' as quota10,
   '当前数量' as quotaName1,
  '冻结数量' as quotaName2,
  '预期年收益率' as quotaName3,
  '' as quotaName4,
  '' as quotaName5,
  '' as quotaName6,
  '' as quotaName7,
  '' as quotaName8,
  '' as quotaName9,
  '' as quotaName10,       
 (select tt.ta_name
  from client.T_BANK_PRODARG tt
  where tt.prodta_no = aa.prodtaNo) as       银行机构  ,
  (select  w.display_value
  from v_dict_business w
  where w.column_name = 'MONEY_TYPE'
  and aa.MONEY_TYPE = w.value) 币种,
  
  BUYDATE          购入日期   ,
  
  PRODBEGINDATE     产品成立日期  ,
  PRODENDDATE       预计产品结束日期  ,
  
          '银行机构' as extendKey1,
          '币种' as extendKey2,
          '购入日期' as extendKey3,
          '产品成立日期' as extendKey4,
          '预计产品结束日期' as extendKey5,
          '1' as operateType,
  '' as batchId,
  rownum as elasticsearchId,
  '05' as elasticsearchTag,
  '银行理财' as elasticsearchTagName,
  sysdate,
  sysdate,
  '' as batchSubId ,
  '','',
  NULL
  from (
select
  fc.ecif_id,
  ll.main_servuserid,
  fc.id_kind,
  fc.id_no,
  ll.client_id,
  a.fund_account,
  fc.client_name  ,
  b.prodta_no as prodtaNo,
  b.prod_code as prodCode,
  b.prod_name as prodName,
  ll.ORGANIZATION_ID,
  to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd') as buyDate,
  a.money_type ,
  sum(nvl(a.current_amount,0)) as currentAmount,
  sum(nvl(a.frozen_amount,0)) as frozenAmount,
  to_char(to_date(b.prod_begin_date, 'yyyymmdd'), 'yyyy-mm-dd') as prodBeginDate,
  to_char(to_date(b.prod_end_date, 'yyyymmdd'), 'yyyy-mm-dd') as prodEndDate,
  to_char(b.prodpre_ratio * 100, '99999999999990.99') as prodpreRatio
from client.t_bank_mshare a, client.t_bank_prodcode b,
  client.t_client_outclientid  ll,
  client.t_client_formalclient fc
  where 1 = 1
  and ll.client_id = a.client_id
  and fc.ecif_id = ll.ecif_id
  and a.prodta_no = b.prodta_no
  and a.prod_code = b.prod_code
  group by  fc.ecif_id,
  ll.main_servuserid,
  fc.id_kind,
  fc.id_no,
  ll.client_id,
  a.fund_account,
  fc.client_name  ,
  b.prodta_no, b.prod_code,b.prod_name, ll.ORGANIZATION_ID, to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd'),
  a.money_type , b.prod_begin_date,b.prod_end_date,b.prodpre_ratio) aa;
 commit; 

-- 证券理财

insert into afaiv.t_es_client_position_tmp
select
  aa.client_id,
  aa.fund_account       as 资金账号,
  aa.ORGANIZATION_ID,
  '' as openDate,
  aa.client_name   as 客户姓名,
  '' as clientTelephone,
       '' as clientEmail,      
  (select org.organization_name from t_xtgl_organization org where aa.ORGANIZATION_ID =org.organization_id )  as 开户营业部,
  '' as mainServOrgId,
       '' as mainServOrgName,
       '' as mainServId,
       '' as mainServHrid,
       '' as mainServName,
       '' as mainServTelephone,
       '' as isAvailable,
   PRODCODE           产品代码 ,
  PRODNAME          产品名称  ,
  '' as userIdStr,
  '' as loginIdStr,
  '' as userNameStr,  
  aa.last_price "T-1日净值",
  nvl(aa.net_value*CURRENTAMOUNT,0) 市值,
  CURRENTAMOUNT     当前数量  ,
  FROZENAMOUNT     冻结数量  ,
  PRODPRERATIO 预期年收益率,
     '' as quota6,
      '' as quota7, 
      '' as quota8,
      '' as quota9, 
      '' as quota10,
   'T-1日净值' as quotaName1,
  '市值' as quotaName2,
  '当前数量' as quotaName3,
  '冻结数量' as quotaName4,
  '预期年收益率' as quotaName5,
  '' as quotaName6,
  '' as quotaName7,
  '' as quotaName8,
  '' as quotaName9,
  '' as quotaName10,       
  (select tt.ta_name
  from client.T_BANK_PRODARG tt
  where tt.prodta_no = aa.prodtaNo AND rownum <= 1) as       "产品TA名称"  ,
  (select  w.display_value
  from v_dict_business w
  where w.column_name = 'MONEY_TYPE'
  and aa.MONEY_TYPE = w.value AND rownum <= 1) 币种,
  
 
  BUYDATE          购入日期   ,
  
  PRODBEGINDATE     产品成立日期  ,
  PRODENDDATE       预计产品结束日期  ,
  
  
  
          '产品TA名称' as extendKey1,
          '币种' as extendKey2,
          '购入日期' as extendKey3,
          '产品成立日期' as extendKey4,
          '预计产品结束日期' as extendKey5,
          '1' as operateType,
  '' as batchId,
  rownum as elasticsearchId,
  '06' as elasticsearchTag,
  '证券理财' as elasticsearchTagName,
  sysdate,
  sysdate,
  '' as batchSubId ,
aa.prodrisk_level as 产品风险等级,
'产品风险等级' as extendKey6, NULL  
  from (
select
  fc.ecif_id,
  ll.main_servuserid,
  fc.id_kind,
  fc.id_no,
  ll.client_id,
  a.fund_account,
  fc.client_name  ,
  b.prodta_no as prodtaNo,
  pr.net_value as last_price,
  b.prod_code as prodCode,
  b.prod_name as prodName,
  ll.ORGANIZATION_ID,
  to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd') as buyDate,
  a.money_type ,
  sum(a.current_amount) as currentAmount,
  sum(a.frozen_amount) as frozenAmount,
  to_char(to_date(b.prod_begin_date, 'yyyymmdd'), 'yyyy-mm-dd') as prodBeginDate,
  to_char(to_date(b.prod_end_date, 'yyyymmdd'), 'yyyy-mm-dd') as prodEndDate,
  to_char(b.prodpre_ratio * 100, '99999999999990.99') as prodpreRatio ,pr.net_Value, max(rl.DISPLAY_VALUE) prodrisk_level
from client.t_bank_secumshare a, client.t_bank_prodcode b,
  client.t_client_outclientid  ll,
  client.t_client_formalclient fc,
  client.t_bank_prodprice pr,
  (select prod_code,prodrisk_level,DISPLAY_VALUE from CLIENT.T_HS08_REALPRODCODE rr,afaer.v_dict_business bb
  where column_name='PRODRISK_LEVEL' and bb.value=prodrisk_level) rl
  where 1 = 1
  and ll.client_id = a.client_id
  and fc.ecif_id = ll.ecif_id
  and a.prod_code = b.prod_code
  AND ( B.PROD_TYPE <> '5' OR (B.PROD_TYPE = '5' AND NVL(B.PRODCODE_KIND,'-1') <> '1') )
  and a.prod_code=pr.prod_code(+)
  and a.prod_code=rl.prod_code(+)
  and pr.INIT_DATE(+)=pkg_org_asset_util.fun_max_exchange_date(sysdate-1)  
  group by  fc.ecif_id,
  ll.main_servuserid,
  fc.id_kind,
  fc.id_no,
  ll.client_id,
  a.fund_account,
  fc.client_name  ,pr.net_value,
  b.prodta_no, b.prod_code,b.prod_name, ll.ORGANIZATION_ID,
  to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd'),
  a.money_type , b.prod_begin_date,b.prod_end_date,b.prodpre_ratio) aa;
commit;

update afaiv.T_ES_CLIENT_POSITION_TMP t set t.client_email=(select EMAIL from client.t_client_outclientid_info f where f.client_id=t.client_id),
                                            t.is_available=(select t3.valid_client from CLIENT.T_INDEX_CLIENTCURENT t3 where t3.client_id=t.client_id),
                                            t.create_time=sysdate,
                                            t.update_time=sysdate,
                                            t.operate_type='1',
                                            t.ELASTICSEARCH_ID=ROWNUM,
                                            t.BATCH_SUB_ID=ceil(ROWNUM / 10000),
                                            t.batch_id=mod(t.batch_sub_id,8)+1;
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
update afaiv.T_ES_CLIENT_POSITION_TMP t set t.ECIF_ID= (select ECIF_ID from client.T_CLIENT_OUTCLIENTID c1 where c1.client_id=t.client_id and rownum<=1);
commit;
   

execute immediate'CREATE TABLE t_tmp_client_Test AS 
SELECT d.client_id
      ,''|'' || listagg(d.user_id, ''|'') within group(ORDER BY d.client_id) || ''|'' AS USER_ID_STR
      ,''|'' || listagg(u.login_id, ''|'') within group(ORDER BY d.client_id) || ''|'' AS login_id_str
      ,''|'' || listagg(u.user_name, ''|'') within group(ORDER BY d.client_id) || ''|'' AS user_name_str
  FROM afaer.t_serv_servrela d, afaer.t_xtgl_user u
 WHERE u.user_id = d.user_id
   AND d.user_relatype = ''2''
   AND u.rec_status = ''1''
 GROUP BY client_id';

execute immediate'create index idx_tmp_client_Test on t_tmp_client_Test(Client_Id)';
execute immediate'create index idx_ES_CLIENT_POSITION_TMP on afaiv.T_ES_CLIENT_POSITION_TMP(Client_Id)';

update afaiv.T_ES_CLIENT_POSITION_TMP t 
   set ( t.user_id_str,t.login_id_str, t.user_name_str) = (select t1.USER_ID_STR,t1.login_id_str, t1.user_name_str 
   from t_tmp_client_Test t1 where  t.client_id=t1.client_id and rownum<=1);
commit;

UPDATE afaiv.t_elastic_job_task t SET t.process_status = 'ready', t.start_time = SYSDATE, t.update_time = SYSDATE WHERE t.title = 'client_position_index' AND t.job_action = 'C';
INSERT INTO afaiv.t_elastic_job_log(title,log_detail,insert_date) VALUES('client_position_index', '1. Oracle procedure completed', SYSDATE);
end pro_esclientpositionhandler;


procedure pro_esusermodularhadler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
execute immediate'create table AFAIV.T_ES_USER_MODULAR_TMP
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
  )';
-- Add comments to the table 
execute immediate'comment on table AFAIV.T_ES_USER_MODULAR_TMP
  is ''用户菜单权限信息''';
-- Add comments to the columns 
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.USER_ID
  is ''员工编号''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.LOGIN_ID
  is ''员工工号''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_ID
  is ''菜单编号''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_NAME
  is ''菜单名称''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_NAME_ALL
  is ''菜单全路径名称（XX中心-XXXX-XXXX）''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.MODULAR_URL
  is ''路径URL''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.OPERATE_TYPE
  is ''1新增；2更新''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.BATCH_ID
  is ''批次号''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_ID
  is ''ES编号''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_TAG
  is ''user_modular_index''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.ELASTICSEARCH_TAG_NAME
  is ''默认"菜单"''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.CREATE_TIME
  is ''插入时间''';
execute immediate'comment on column AFAIV.T_ES_USER_MODULAR_TMP.UPDATE_TIME
  is ''更新时间''';


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

end pro_esusermodularhadler;


procedure pro_esuserhelphandler(o_code OUT NUMBER
   ,o_note OUT VARCHAR2) is
begin
execute immediate'CREATE TABLE afaiv.T_ES_USER_HELP_TMP(
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
)';

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
