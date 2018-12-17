CREATE OR REPLACE PACKAGE BODY pkg_pro_handle_afadiv IS

  PROCEDURE pro_esclientbasehandler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2

  ) IS
  BEGIN
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esclientbasehandler start');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.t_es_client_base_tmp';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP1';
    INSERT INTO afaiv.esclientbasehandler_temp1
      SELECT a.client_id
            ,f.fund_account
            ,f.branch_no AS client_org_id
            ,f.open_date
            ,(SELECT o.organization_name
                FROM afaer.t_xtgl_organization o
               WHERE o.organization_id = f.branch_no
                 AND o.rec_status = '1'
                 AND rownum <= 1) AS client_org_name
            ,'1' AS operate_type
            ,'client_base_index' AS elasticsearch_tag
            ,'客户' AS elasticsearch_tag_name
            ,SYSDATE AS create_time
            ,SYSDATE AS update_time
        FROM client.t_client_counterclient a, client.t_client_fundaccount f
       WHERE a.client_id = f.client_id
         AND f.account_flag = '1';
  
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP2';
    INSERT INTO afaiv.esclientbasehandler_temp2
      SELECT a.*, b.client_name, b.phonecode AS client_telephone, b.email AS client_email
        FROM afaiv.esclientbasehandler_temp1 a, client.t_client_outclientid_info b
       WHERE a.client_id = b.client_id;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP1';
  
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP3';
    INSERT INTO afaiv.esclientbasehandler_temp3
      SELECT a.*, b.main_servuserid AS main_serv_id
        FROM afaiv.esclientbasehandler_temp2 a, client.t_client_outclientid b
       WHERE a.client_id = b.client_id;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP2';
  
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP4';
    INSERT INTO afaiv.esclientbasehandler_temp4
      SELECT a.*, b.login_id AS main_serv_hrid, b.user_name AS main_serv_name, b.phonecode AS main_serv_telephone
        FROM afaiv.esclientbasehandler_temp3 a, afaer.t_xtgl_user b
       WHERE a.main_serv_id = b.user_id;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP3';
  
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP5';
    INSERT INTO afaiv.esclientbasehandler_temp5
      SELECT a.*
            ,b.organization_id   AS main_serv_org_id
            ,b.organization_name AS main_serv_org_name
            ,NULL
            ,NULL
            ,NULL
            ,NULL
        FROM afaiv.esclientbasehandler_temp4 a, afaer.t_xtgl_organization b
       WHERE a.client_org_id = b.organization_id;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP4';
  
    INSERT INTO afaiv.t_es_client_base_tmp
      SELECT a.*, NULL FROM afaiv.esclientbasehandler_temp5 a;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.esclientbasehandler_TEMP5';
  
    UPDATE afaiv.t_es_client_base_tmp t SET t.batch_sub_id = ceil(rownum / 10000);
    UPDATE afaiv.t_es_client_base_tmp t
       SET t.batch_id         = MOD(t.batch_sub_id, 8) + 1
          ,t.elasticsearch_id = rownum
          ,t.is_available    =
           (SELECT t3.valid_client
              FROM client.t_index_clientcurent t3
             WHERE t3.client_id = t.client_id
               AND rownum <= 1);
    COMMIT;
    UPDATE afaiv.t_es_client_base_tmp t
       SET t.main_serv_telephone =
           (SELECT u.phonecode
              FROM afaer.t_xtgl_user u, afaer.t_serv_servrela d
             WHERE d.client_id = t.client_id
               AND u.user_id = d.user_id
               AND rownum <= 1);
    COMMIT;
    UPDATE afaiv.t_es_client_base_tmp t
       SET t.client_email =
           (SELECT email FROM client.t_client_outclientid_info f WHERE f.client_id = t.client_id);
    COMMIT;
    UPDATE afaiv.t_es_client_base_tmp t
       SET t.ecif_id =
           (SELECT ecif_id
              FROM client.t_client_outclientid c1
             WHERE c1.client_id = t.client_id
               AND rownum <= 1);
    COMMIT;
    INSERT INTO afaiv.t_elastic_job_log (title, log_detail, insert_date)
           VALUES ('client_base_index', '1. Oracle procedure completed', SYSDATE);
    COMMIT;
    --时间标记----
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esclientbasehandler end');
  
  END pro_esclientbasehandler;

  PROCEDURE pro_esclientpositionhandler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  ) IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.T_ES_CLIENT_POSITION_TMP';
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') || ': 1. pro_esclientpositionhandler begin');
  
    -- 二级持仓
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT a.client_id
            ,a.fund_account AS 资金账号
            ,a.branch_no
            ,'' AS opendate
            ,a.client_name AS 客户姓名
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE a.branch_no = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,to_char(a.stock_code) AS 证券代码
            ,(SELECT s.stock_name
                FROM quser.t_hs06_stkcode s
               WHERE a.stock_code = s.stock_code
                 AND a.exchange_type = s.exchange_type) 证券名称
            ,'' AS useridstr
            ,'' AS loginidstr
            ,'' AS usernamestr
            ,a.current_amount AS "当前余额"
             ,(a.current_amount - a.frozen_amount + a.unfrozen_amount)  AS "可用余额"
             ,(a.cost_price) AS "买入均价(元)"
             ,(a.asset_price) AS "昨日收盘价(元)"
             ,market_value AS "市值(元)"
             ,((a.current_amount + a.correct_amount) * a.asset_price - a.sum_buy_balance + a.sum_sell_balance) AS "盈亏值(元)"
             ,(round(decode(a.sum_buy_balance, 0, 0,
                               ((a.current_amount + a.correct_amount) * a.asset_price - a.sum_buy_balance +
                                a.sum_sell_balance) / a.sum_buy_balance), 2) * 100 ) AS "盈亏率%"
             ,'' AS quota8
             ,'' AS quota9
             ,'' AS quota10
             ,'当前余额' AS quotaname1
             ,'可用余额' AS quotaname2
             ,'买入均价(元)' AS quotaname3
             ,'昨日收盘价(元)' AS quotaname4
             ,'市值(元)' AS quotaname5
             ,'盈亏值(元)' AS quotaname6
             ,'盈亏率%' AS quotaname7
             ,'' AS quotaname8
             ,'' AS quotaname9
             ,'' AS quotaname10
             ,to_char(a.init_date) AS "时间戳"
             ,b.placename AS "所属行业"
             ,(SELECT w.display_value
                FROM v_dict_business w
               WHERE w.column_name = 'EXCHANGE_TYPE'
                 AND exchange_type = w.value) AS "市场类别"
             ,'' AS extendvalue4
             ,'' AS extendvalue5
             ,'时间戳' AS extendkey1
             ,'所属行业' AS extendkey2
             ,'市场类别' AS extendkey3
             ,'' AS extendkey4
             ,'' AS extendkey5
             ,'1' AS operatetype
             ,'' AS batchid
             ,rownum AS elasticsearchid
             ,'01' AS elasticsearchtag
             ,'二级持仓' AS elasticsearchtagname
             ,SYSDATE
             ,SYSDATE
             ,'' AS batchsubid
             ,''
             ,''
             ,''
        FROM (SELECT a.branch_no
                    ,a.init_date
                    ,fc.ecif_id
                    ,ll.main_servuserid
                    ,fc.id_kind
                    ,fc.id_no
                    ,ll.client_id
                    ,a.fund_account
                    ,a.stock_account
                    ,fc.client_name
                    ,a.stock_code
                    ,a.current_amount
                    ,a.correct_amount
                    ,a.frozen_amount
                    ,a.unfrozen_amount
                    ,a.cost_price
                    ,CAST(nvl((a.current_amount + a.correct_amount) * pr.asset_price * er.turn_rmb, 0) AS NUMBER(20, 2)) market_value
                    ,nvl(pr.asset_price, 0) asset_price
                    ,a.sum_buy_balance
                    ,a.sum_sell_balance
                    ,a.exchange_type
                    ,a.stock_type
                    ,a.money_type
                FROM client.t_hs06_stock          a
                    ,client.t_client_outclientid  ll
                    ,client.t_client_formalclient fc
                    ,quser.t_hs06_price           pr
                    ,quser.t_pub_exchange_rate    er
               WHERE 1 = 1
                 AND ll.client_id = a.client_id
                 AND fc.ecif_id = ll.ecif_id
                 AND a.stock_code = pr.stock_code(+)
                 AND a.exchange_type = pr.exchange_type(+)
                 AND decode(er.curr_type_cd, 'CNY', '0', 'USD', '1', 'HKD', '2') = a.money_type
                 AND er.star_dt = substr(a.init_date, 1, 4) || '0101') a
        LEFT JOIN afaer.v_seccode_with_platename b
          ON a.stock_code = b.seccode;
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 2. pro_esclientpositionhandler common stock completed');
    -- 场外开基
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT aa.client_id
            ,aa.fund_account
            ,aa.branch_no
            ,'' AS opendate
            ,'' AS clientname
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE aa.branch_no = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,to_char(aa.fund_code) AS 基金代码
            ,(SELECT c.fund_name FROM quser.t_hs06_ofstkcode c WHERE aa.fund_code = c.fund_code) AS 基金名称
            ,'' AS useridstr
            ,'' AS loginidstr
            ,'' AS usernamestr
            ,aa.jjfe AS 基金份额
            ,aa.jrjz AS "最新净值(元)"
             ,(nvl(aa.yke, 0) ) AS "盈亏额(元)"
             ,(decode(aa.buy_money, 0, 0, (nvl(aa.yke, 0) / aa.buy_money) * 100)) AS "盈亏率"
             ,aa.jjsz AS "基金市值(元)"
             ,aa.jjkyfe AS 基金可用份额
             ,'' AS quota7
             ,'' AS quota8
             ,'' AS quota9
             ,'' AS quota10
             ,'基金份额' AS quotaname1
             ,'最新净值(元)' AS quotaname2
             ,'盈亏额(元)' AS quotaname3
             ,'盈亏率' AS quotaname4
             ,'基金市值(元)' AS quotaname5
             ,'基金可用份额' AS quotaname6
             ,'' AS quotaname7
             ,'' AS quotaname8
             ,'' AS quotaname9
             ,'' AS quotaname10
             ,(SELECT d.fund_name FROM quser.t_fund_company d WHERE aa.fund_company = d.fund_company) AS 基金公司
             ,(SELECT w.display_value
                FROM v_dict_business w
               WHERE w.column_name = 'FUND_TYPE'
                 AND aa.fund_type = w.value) AS 基金类型
             ,aa.prodrisk_level AS "产品风险等级"
             ,'' AS extendvalue4
             ,'' AS extendvalue5
             ,'基金公司' AS extendkey1
             ,'基金类型' AS extendkey2
             ,'产品风险等级' AS extendkey3
             ,'' AS extendkey4
             ,'' AS extendkey5
             ,'1' AS operatetype
             ,'' AS batchid
             ,rownum AS elasticsearchid
             ,'02' AS elasticsearchtag
             ,'场外开基' AS elasticsearchtagname
             ,SYSDATE
             ,SYSDATE
             ,'' AS batchsubid
             ,''
             ,''
             ,NULL
        FROM (SELECT a.fund_account
                    ,a.client_id
                    ,a.fund_code
                    ,fi.stock_type fund_type
                    ,a.branch_no
                    ,a.fund_company
                    ,SUM((nvl(a.current_share, 0) - nvl(a.back_share, 0)) * a.cost_price) AS buy_money
                    ,SUM((nvl(f.nav, 0) - nvl(a.cost_price, 0)) * (nvl(a.current_share, 0) - nvl(a.back_share, 0))) AS yke
                    ,SUM(nvl(a.current_share, 0) - nvl(a.back_share, 0)) jjfe
                    ,SUM(nvl(a.current_share, 0) - nvl(a.back_share, 0) - nvl(a.frozen_share, 0)) jjkyfe
                    ,nvl(f.nav, 0) jrjz
                    ,SUM((nvl(a.current_share, 0) - nvl(a.back_share, 0)) * nvl(f.nav, 0)) AS jjsz
                    ,MAX(rl.display_value) prodrisk_level
                FROM (SELECT a.fund_account fund_account
                            ,a.client_id client_id
                            ,a.secum_account stock_account
                            ,a.branch_no branch_no
                            ,a.prod_code fund_code
                            ,a.buy_date buy_date
                            ,a.net_no net_no
                            ,a.money_type money_type
                            ,a.allot_no allotno
                            ,a.begin_amount begin_share
                            ,a.current_amount current_share
                            ,a.dividend_way auto_buy
                            ,a.charge_type charge_type
                            ,a.secum_market_value market_value
                            ,a.trans_account trans_account
                            ,NULL back_share
                            ,NULL business_frozen_share
                            ,NULL correct_share
                            ,a.prod_cost_price cost_price
                            ,NULL frozen_share
                            ,a.prod_ta_no fund_company
                            ,NULL ibranch_no
                            ,a.oc_date init_date
                            ,NULL long_frozen_share
                            ,NULL position_str
                            ,NULL stock_status
                            ,NULL stock_type
                            ,NULL ta_no
                            ,a.oc_date oc_date
                            ,nvl(a.sum_buy_balance, 0) sum_buy_balance
                            ,nvl(a.sum_sell_balance, 0) sum_sell_balance
                        FROM client.t_bank_secumshare a
                       WHERE EXISTS (SELECT 1
                                FROM client.t_bank_prodcode pc
                               WHERE a.prod_ta_no = pc.prodta_no
                                 AND pc.prod_type = '5'
                                 AND pc.prodcode_kind = '1')) a
                    ,client.t_client_outclientid ll
                    ,quser.t_hs06_ofstkcode fi
                    ,client.t_client_formalclient fc
                    ,(SELECT substr(s.prodta_no, 1, 4) AS fund_company
                            ,substr(s.prod_code, 1, 6) AS fund_code
                            ,s.net_value AS nav
                        FROM client.t_bank_prodprice s
                       WHERE EXISTS (SELECT 1
                                FROM client.t_bank_prodcode pc
                               WHERE s.prodta_no = pc.prodta_no
                                 AND pc.prod_type = '5'
                                 AND pc.prodcode_kind = '1')
                         AND s.oc_date = (SELECT MAX(oc_date) FROM client.t_bank_prodprice)) f
                    ,(SELECT prod_code, prodrisk_level, display_value
                        FROM client.t_hs08_realprodcode rr, afaer.v_dict_business bb
                       WHERE column_name = 'PRODRISK_LEVEL'
                         AND bb.value = prodrisk_level) rl
               WHERE 1 = 1
                 AND ll.client_id = a.client_id
                 AND fc.ecif_id = ll.ecif_id
                 AND a.fund_code = fi.fund_code
                 AND a.fund_code = rl.prod_code(+)
                 AND a.fund_company = f.fund_company(+)
                 AND a.fund_code = f.fund_code(+)
                 AND a.fund_code NOT IN (SELECT product_code FROM afaer.v_prod_setfinancial)
               GROUP BY a.branch_no, a.fund_code, a.fund_account, a.client_id, a.fund_company, f.nav, fi.stock_type) aa;
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 3. pro_esclientpositionhandler otc fund completed');
    -- 兴业资管  
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT aa.client_id
            ,aa.fund_account
            ,aa.branch_no
            ,'' AS opendate
            ,'' AS clientname
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE aa.branch_no = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,to_char(aa.fund_code) AS 基金代码
            ,(SELECT c.fund_name FROM quser.t_hs06_ofstkcode c WHERE aa.fund_code = c.fund_code) AS 基金名称
            ,'' AS useridstr
            ,'' AS loginidstr
            ,'' AS usernamestr
            ,aa.jjfe AS 基金份额
            ,aa.jrjz AS "最新净值(元)"
             ,(nvl(aa.yke, 0) ) AS "盈亏额(元)"
             ,(decode(aa.buy_money, 0, 0, (nvl(aa.yke, 0) / aa.buy_money) * 100)) AS "盈亏率"
             ,aa.jjsz AS "基金市值(元)"
             ,aa.jjkyfe AS 基金可用份额
             ,'' AS quota7
             ,'' AS quota8
             ,'' AS quota9
             ,'' AS quota10
             ,'基金份额' AS quotaname1
             ,'最新净值(元)' AS quotaname2
             ,'盈亏额(元)' AS quotaname3
             ,'盈亏率' AS quotaname4
             ,'基金市值(元)' AS quotaname5
             ,'基金可用份额' AS quotaname6
             ,'' AS quotaname7
             ,'' AS quotaname8
             ,'' AS quotaname9
             ,'' AS quotaname10
             ,(SELECT d.fund_name FROM quser.t_fund_company d WHERE aa.fund_company = d.fund_company) AS 基金公司
             ,(SELECT w.display_value
                FROM v_dict_business w
               WHERE w.column_name = 'FUND_TYPE'
                 AND aa.fund_type = w.value) AS 基金类型
             ,aa.prodrisk_level AS "产品风险等级"
             ,'' AS extendvalue4
             ,'' AS extendvalue5
             ,'基金公司' AS extendkey1
             ,'基金类型' AS extendkey2
             ,'产品风险等级' AS extendkey3
             ,'' AS extendkey4
             ,'' AS extendkey5
             ,'1' AS operatetype
             ,'' AS batchid
             ,rownum AS elasticsearchid
             ,'03' AS elasticsearchtag
             ,'兴业资管' AS elasticsearchtagname
             ,SYSDATE
             ,SYSDATE
             ,'' AS batchsubid
             ,''
             ,''
             ,NULL
        FROM (SELECT a.fund_account
                    ,a.client_id
                    ,a.fund_code
                    ,fi.stock_type fund_type
                    ,a.branch_no
                    ,a.fund_company
                    ,SUM((nvl(a.current_share, 0) - nvl(a.back_share, 0)) * a.cost_price) AS buy_money
                    ,SUM((nvl(f.nav, 0) - nvl(a.cost_price, 0)) * (nvl(a.current_share, 0) - nvl(a.back_share, 0))) AS yke
                    ,SUM(nvl(a.current_share, 0) - nvl(a.back_share, 0)) jjfe
                    ,SUM(nvl(a.current_share, 0) - nvl(a.back_share, 0) - nvl(a.frozen_share, 0)) jjkyfe
                    ,nvl(f.nav, 0) jrjz
                    ,SUM((nvl(a.current_share, 0) - nvl(a.back_share, 0)) * nvl(f.nav, 0)) AS jjsz
                    ,MAX(rl.display_value) prodrisk_level
                FROM (SELECT a.fund_account fund_account
                            ,a.client_id client_id
                            ,a.secum_account stock_account
                            ,a.branch_no branch_no
                            ,a.prod_code fund_code
                            ,a.buy_date buy_date
                            ,a.net_no net_no
                            ,a.money_type money_type
                            ,a.allot_no allotno
                            ,a.begin_amount begin_share
                            ,a.current_amount current_share
                            ,a.dividend_way auto_buy
                            ,a.charge_type charge_type
                            ,a.secum_market_value market_value
                            ,a.trans_account trans_account
                            ,NULL back_share
                            ,NULL business_frozen_share
                            ,NULL correct_share
                            ,a.prod_cost_price cost_price
                            ,NULL frozen_share
                            ,a.prod_ta_no fund_company
                            ,NULL ibranch_no
                            ,a.oc_date init_date
                            ,NULL long_frozen_share
                            ,NULL position_str
                            ,NULL stock_status
                            ,NULL stock_type
                            ,NULL ta_no
                            ,a.oc_date oc_date
                            ,nvl(a.sum_buy_balance, 0) sum_buy_balance
                            ,nvl(a.sum_sell_balance, 0) sum_sell_balance
                        FROM client.t_bank_secumshare a
                       WHERE EXISTS (SELECT 1
                                FROM client.t_bank_prodcode pc
                               WHERE a.prod_ta_no = pc.prodta_no
                                 AND pc.prod_type = '5'
                                 AND pc.prodcode_kind = '1')) a
                    ,client.t_client_outclientid ll
                    ,quser.t_hs06_ofstkcode fi
                    ,client.t_client_formalclient fc
                    ,(SELECT substr(s.prodta_no, 1, 4) AS fund_company
                            ,substr(s.prod_code, 1, 6) AS fund_code
                            ,s.net_value AS nav
                        FROM client.t_bank_prodprice s
                       WHERE EXISTS (SELECT 1
                                FROM client.t_bank_prodcode pc
                               WHERE s.prodta_no = pc.prodta_no
                                 AND pc.prod_type = '5'
                                 AND pc.prodcode_kind = '1')
                         AND s.oc_date = (SELECT MAX(oc_date) FROM client.t_bank_prodprice)) f
                    ,(SELECT prod_code, prodrisk_level, display_value
                        FROM client.t_hs08_realprodcode rr, afaer.v_dict_business bb
                       WHERE column_name = 'PRODRISK_LEVEL'
                         AND bb.value = prodrisk_level) rl
               WHERE 1 = 1
                 AND ll.client_id = a.client_id
                 AND fc.ecif_id = ll.ecif_id
                 AND a.fund_code = fi.fund_code
                 AND a.fund_code = rl.prod_code(+)
                 AND a.fund_company = f.fund_company(+)
                 AND a.fund_code = f.fund_code(+)
                 AND a.fund_code IN (SELECT product_code FROM afaer.v_prod_setfinancial)
               GROUP BY a.branch_no, a.fund_code, a.fund_account, a.client_id, a.fund_company, f.nav, fi.stock_type) aa;
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 4. pro_esclientpositionhandler xyzq asset completed');
  
    -- 银行理财
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT aa.client_id
            ,aa.fund_account AS 资金账号
            ,aa.organization_id
            ,'' AS opendate
            ,aa.client_name AS 客户姓名
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE aa.organization_id = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,prodcode 产品代码
            ,prodname 产品名称
            ,'' AS useridstr
            ,'' AS loginidstr
            ,'' AS usernamestr
            ,currentamount 当前数量
            ,frozenamount 冻结数量
            ,prodpreratio 预期年收益率
            ,'' AS quota4
            ,'' AS quota5
            ,'' AS quota6
            ,'' AS quota7
            ,'' AS quota8
            ,'' AS quota9
            ,'' AS quota10
            ,'当前数量' AS quotaname1
            ,'冻结数量' AS quotaname2
            ,'预期年收益率' AS quotaname3
            ,'' AS quotaname4
            ,'' AS quotaname5
            ,'' AS quotaname6
            ,'' AS quotaname7
            ,'' AS quotaname8
            ,'' AS quotaname9
            ,'' AS quotaname10
            ,(SELECT tt.ta_name FROM client.t_bank_prodarg tt WHERE tt.prodta_no = aa.prodtano) AS 银行机构
            ,(SELECT w.display_value
                FROM v_dict_business w
               WHERE w.column_name = 'MONEY_TYPE'
                 AND aa.money_type = w.value) 币种
            ,buydate 购入日期
            ,prodbegindate 产品成立日期
            ,prodenddate 预计产品结束日期
            ,'银行机构' AS extendkey1
            ,'币种' AS extendkey2
            ,'购入日期' AS extendkey3
            ,'产品成立日期' AS extendkey4
            ,'预计产品结束日期' AS extendkey5
            ,'1' AS operatetype
            ,'' AS batchid
            ,rownum AS elasticsearchid
            ,'05' AS elasticsearchtag
            ,'银行理财' AS elasticsearchtagname
            ,SYSDATE
            ,SYSDATE
            ,'' AS batchsubid
            ,''
            ,''
            ,NULL
        FROM (SELECT fc.ecif_id
                    ,ll.main_servuserid
                    ,fc.id_kind
                    ,fc.id_no
                    ,ll.client_id
                    ,a.fund_account
                    ,fc.client_name
                    ,b.prodta_no AS prodtano
                    ,b.prod_code AS prodcode
                    ,b.prod_name AS prodname
                    ,ll.organization_id
                    ,to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd') AS buydate
                    ,a.money_type
                    ,SUM(nvl(a.current_amount, 0)) AS currentamount
                    ,SUM(nvl(a.frozen_amount, 0)) AS frozenamount
                    ,to_char(to_date(b.prod_begin_date, 'yyyymmdd'), 'yyyy-mm-dd') AS prodbegindate
                    ,to_char(to_date(b.prod_end_date, 'yyyymmdd'), 'yyyy-mm-dd') AS prodenddate
                    ,to_char(b.prodpre_ratio * 100, '99999999999990.99') AS prodpreratio
                FROM client.t_bank_mshare         a
                    ,client.t_bank_prodcode       b
                    ,client.t_client_outclientid  ll
                    ,client.t_client_formalclient fc
               WHERE 1 = 1
                 AND ll.client_id = a.client_id
                 AND fc.ecif_id = ll.ecif_id
                 AND a.prodta_no = b.prodta_no
                 AND a.prod_code = b.prod_code
               GROUP BY fc.ecif_id
                       ,ll.main_servuserid
                       ,fc.id_kind
                       ,fc.id_no
                       ,ll.client_id
                       ,a.fund_account
                       ,fc.client_name
                       ,b.prodta_no
                       ,b.prod_code
                       ,b.prod_name
                       ,ll.organization_id
                       ,to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd')
                       ,a.money_type
                       ,b.prod_begin_date
                       ,b.prod_end_date
                       ,b.prodpre_ratio) aa;
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 5. pro_esclientpositionhandler bank fin completed');
  
    -- 融资融券持仓 
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT a.client_id
            ,a.fund_account AS fundaccount
            ,a.branch_no
            ,'' AS opendate
            ,'' AS clientname
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE a.branch_no = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,TRIM(a.stock_code) AS "stockCode"
             ,b.stock_name AS "stockName"
             ,'' AS useridstr
             ,'' AS loginidstr
             ,'' AS usernamestr
             ,to_char(a.cost_price) AS "costPrice"
             , ---成本价 
             to_char(a.current_amount) AS "currentAmount"
             , --股票余额
             to_char(a.current_amount + a.unfrozen_amount - a.frozen_amount) kyamount
             , --可用余额=股票余额+解冻数量-冻结数量 
             to_char(b.last_price) AS "assetPrice"
             , --市价 
             to_char(b.last_price * a.current_amount) AS "currentBalance"
             , --当前市值=股票余额*市价 
             (((a.current_amount + a.correct_amount) * b.last_price - a.sum_buy_balance + a.sum_sell_balance)) AS "ykBalance"
             , --盈亏额
             CASE a.sum_buy_balance
               WHEN 0 THEN
                0
               ELSE
                (((((a.current_amount + a.correct_amount) * b.last_price - a.sum_buy_balance + a.sum_sell_balance) /
                     a.sum_buy_balance)) * 100 )
             END AS "ykRate"
             , --盈亏率=盈亏额/累计买入金额
             '' AS quota8
             ,'' AS quota9
             ,'' AS quota10
             ,'成本价(元)' AS quotaname1
             ,'股票余额' AS quotaname2
             ,'可用余额' AS quotaname3
             ,'盈亏率' AS quotaname4
             ,'基金市值(元)' AS quotaname5
             ,'盈亏额(元)' AS quotaname6
             ,'基金可用份额' AS quotaname7
             ,'' AS quotaname8
             ,'' AS quotaname9
             ,'' AS quotaname10
             ,a.stock_account AS extendvalue1
             , --股东账户
             a.exchange_type AS extendvalue2
             , --交易类别 
             a.fund_account AS extendvalue3
             ,'' AS extendvalue4
             ,'' AS extendvalue5
             ,'股东账户' AS extendkey1
             ,'交易类别' AS extendkey2
             ,'信用账号' AS extendkey3
             ,'' AS extendkey4
             ,'' AS extendkey5
             ,'1' AS operatetype
             ,'' AS batchid
             ,rownum AS elasticsearchid
             ,'04' AS elasticsearchtag
             ,'融资融券持仓' AS elasticsearchtagname
             ,SYSDATE
             ,SYSDATE
             ,'' AS batchsubid
             ,''
             ,''
             ,''
        FROM client.t_fina_stock a, quser.t_hs06_stkcode b
       WHERE TRIM(a.stock_code) = b.stock_code(+)
         AND a.exchange_type = b.exchange_type(+);
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 6. pro_esclientpositionhandler fin and lend completed');
  
    -- 证券理财
  
    INSERT INTO afaiv.t_es_client_position_tmp
      SELECT aa.client_id
            ,aa.fund_account AS 资金账号
            ,aa.organization_id
            ,'' AS opendate
            ,aa.client_name AS 客户姓名
            ,'' AS clienttelephone
            ,'' AS clientemail
            ,(SELECT org.organization_name FROM t_xtgl_organization org WHERE aa.organization_id = org.organization_id) AS 开户营业部
            ,'' AS mainservorgid
            ,'' AS mainservorgname
            ,'' AS mainservid
            ,'' AS mainservhrid
            ,'' AS mainservname
            ,'' AS mainservtelephone
            ,'' AS isavailable
            ,prodcode 产品代码
            ,prodname 产品名称
            ,'' AS useridstr
            ,'' AS loginidstr
            ,'' AS usernamestr
            ,aa.last_price "T-1日净值"
             ,nvl(aa.net_value * currentamount, 0) 市值
             ,currentamount 当前数量
             ,frozenamount 冻结数量
             ,prodpreratio 预期年收益率
             ,'' AS quota6
             ,'' AS quota7
             ,'' AS quota8
             ,'' AS quota9
             ,'' AS quota10
             ,'T-1日净值' AS quotaname1
             ,'市值' AS quotaname2
             ,'当前数量' AS quotaname3
             ,'冻结数量' AS quotaname4
             ,'预期年收益率' AS quotaname5
             ,'' AS quotaname6
             ,'' AS quotaname7
             ,'' AS quotaname8
             ,'' AS quotaname9
             ,'' AS quotaname10
             ,(SELECT tt.ta_name
                FROM client.t_bank_prodarg tt
               WHERE tt.prodta_no = aa.prodtano
                 AND rownum <= 1) AS "产品TA名称"
             ,(SELECT w.display_value
                FROM v_dict_business w
               WHERE w.column_name = 'MONEY_TYPE'
                 AND aa.money_type = w.value
                 AND rownum <= 1) 币种
             ,buydate 购入日期
             ,prodbegindate 产品成立日期
             ,prodenddate 预计产品结束日期
             ,'产品TA名称' AS extendkey1
             ,'币种' AS extendkey2
             ,'购入日期' AS extendkey3
             ,'产品成立日期' AS extendkey4
             ,'预计产品结束日期' AS extendkey5
             ,'1' AS operatetype
             ,'' AS batchid
             ,rownum AS elasticsearchid
             ,'06' AS elasticsearchtag
             ,'证券理财' AS elasticsearchtagname
             ,SYSDATE
             ,SYSDATE
             ,'' AS batchsubid
             ,aa.prodrisk_level AS 产品风险等级
             ,'产品风险等级' AS extendkey6
             ,NULL
        FROM (SELECT fc.ecif_id
                    ,ll.main_servuserid
                    ,fc.id_kind
                    ,fc.id_no
                    ,ll.client_id
                    ,a.fund_account
                    ,fc.client_name
                    ,b.prodta_no AS prodtano
                    ,pr.net_value AS last_price
                    ,b.prod_code AS prodcode
                    ,b.prod_name AS prodname
                    ,ll.organization_id
                    ,to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd') AS buydate
                    ,a.money_type
                    ,SUM(a.current_amount) AS currentamount
                    ,SUM(a.frozen_amount) AS frozenamount
                    ,to_char(to_date(b.prod_begin_date, 'yyyymmdd'), 'yyyy-mm-dd') AS prodbegindate
                    ,to_char(to_date(b.prod_end_date, 'yyyymmdd'), 'yyyy-mm-dd') AS prodenddate
                    ,to_char(b.prodpre_ratio * 100, '99999999999990.99') AS prodpreratio
                    ,pr.net_value
                    ,MAX(rl.display_value) prodrisk_level
                FROM client.t_bank_secumshare a
                    ,client.t_bank_prodcode b
                    ,client.t_client_outclientid ll
                    ,client.t_client_formalclient fc
                    ,client.t_bank_prodprice pr
                    ,(SELECT prod_code, prodrisk_level, display_value
                        FROM client.t_hs08_realprodcode rr, afaer.v_dict_business bb
                       WHERE column_name = 'PRODRISK_LEVEL'
                         AND bb.value = prodrisk_level) rl
               WHERE ll.client_id = a.client_id
                 AND fc.ecif_id = ll.ecif_id
                 AND a.prod_code = b.prod_code
                 AND (b.prod_type <> '5' OR (b.prod_type = '5' AND nvl(b.prodcode_kind, '-1') <> '1'))
                 AND a.prod_code = pr.prod_code(+)
                 AND a.prod_code = rl.prod_code(+)
                 AND pr.init_date(+) = pkg_org_asset_util.fun_max_exchange_date(SYSDATE - 1)
               GROUP BY fc.ecif_id
                       ,ll.main_servuserid
                       ,fc.id_kind
                       ,fc.id_no
                       ,ll.client_id
                       ,a.fund_account
                       ,fc.client_name
                       ,pr.net_value
                       ,b.prodta_no
                       ,b.prod_code
                       ,b.prod_name
                       ,ll.organization_id
                       ,to_char(to_date(a.buy_date, 'yyyymmdd'), 'yyyy-mm-dd')
                       ,a.money_type
                       ,b.prod_begin_date
                       ,b.prod_end_date
                       ,b.prodpre_ratio) aa;
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 7. pro_esclientpositionhandler sec fin completed');
  
    UPDATE afaiv.t_es_client_position_tmp t
       SET t.client_email    =
           (SELECT email
              FROM client.t_client_outclientid_info f
             WHERE f.client_id = t.client_id
               AND rownum <= 1)
          ,t.is_available    =
           (SELECT t3.valid_client
              FROM client.t_index_clientcurent t3
             WHERE t3.client_id = t.client_id
               AND rownum <= 1)
          ,t.create_time      = SYSDATE
          ,t.update_time      = SYSDATE
          ,t.operate_type     = '1'
          ,t.elasticsearch_id = rownum
          ,t.batch_sub_id     = ceil(rownum / 10000)
          ,t.batch_id         = MOD(t.batch_sub_id, 8) + 1;
    COMMIT;
    
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 8. pro_esclientpositionhandler update part 0.1 completed');
                         
    UPDATE afaiv.t_es_client_position_tmp t
       SET (t.ecif_id, t.main_serv_id) =
           (SELECT t5.ecif_id, t5.main_servuserid
              FROM client.t_client_outclientid t5
             WHERE t5.client_id = t.client_id
               AND rownum <= 1);
         
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 8. pro_esclientpositionhandler update part 0.2 completed');      
  
    UPDATE afaiv.t_es_client_position_tmp t
       SET (t.main_serv_hrid, t.main_serv_name, t.main_serv_telephone, t.main_serv_org_id,t.main_serv_org_name) =
           (SELECT u.hrid, u.user_name, u.phonecode, u.organization_id, t4.organization_name
              FROM afaer.t_xtgl_user u, afaer.t_xtgl_organization t4
             WHERE u.user_id = t.main_serv_id
               AND t4.organization_id = u.organization_id
               AND rownum <= 1)
     WHERE t.main_serv_id IS NOT NULL;
    COMMIT;
    
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 8. pro_esclientpositionhandler update part 0.3 completed');   
    UPDATE afaiv.t_es_client_position_tmp t
       SET t.open_date =
           (SELECT b.open_date
              FROM client.t_client_fundaccount b
             WHERE b.client_id = t.client_id
               AND rownum <= 1);
    COMMIT;
    
     dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 8. pro_esclientpositionhandler update part 0.4 completed');  
                         
    UPDATE afaiv.t_es_client_position_tmp t
       SET t.client_telephone =
           (SELECT b.phonecode
              FROM client.t_client_outclientid_info b
             WHERE b.client_id = t.client_id
               AND rownum <= 1);
    COMMIT;
  
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') ||
                         ': 8. pro_esclientpositionhandler update part 1 completed');
  
    EXECUTE IMMEDIATE 'truncate table afaiv.t_tmp_client_Test';
    INSERT INTO afaiv.t_tmp_client_test
      SELECT d.client_id
            ,'|' || listagg(d.user_id, '|') within GROUP(ORDER BY d.client_id) || '|' AS user_id_str
            ,'|' || listagg(u.login_id, '|') within GROUP(ORDER BY d.client_id) || '|' AS login_id_str
            ,'|' || listagg(u.user_name, '|') within GROUP(ORDER BY d.client_id) || '|' AS user_name_str
        FROM afaer.t_serv_servrela d, afaer.t_xtgl_user u
       WHERE u.user_id = d.user_id
         AND d.user_relatype = '2'
         AND u.rec_status = '1'
       GROUP BY client_id;
  
    EXECUTE IMMEDIATE 'create index afaiv.idx_tmp_client_Test on afaiv.t_tmp_client_Test(Client_Id)';

    UPDATE afaiv.t_es_client_position_tmp t
       SET (t.user_id_str, t.login_id_str, t.user_name_str) =
           (SELECT t1.user_id_str, t1.login_id_str, t1.user_name_str
              FROM afaiv.t_tmp_client_test t1
             WHERE t.client_id = t1.client_id
               AND rownum <= 1);
    COMMIT;
    
    --思考为什么这样???
    UPDATE afaiv.t_es_client_position_tmp t
       SET t.batch_id = MOD(t.batch_sub_id, 8) + 1;
    COMMIT;
    INSERT INTO afaiv.t_elastic_job_log (title, log_detail, insert_date)
           VALUES ('client_position', '1. Oracle procedure completed', SYSDATE);
    COMMIT;
    dbms_output.put_line(to_char(SYSDATE, 'YYYYMMDD HH24:MI:SS') || ': 10. pro_esclientpositionhandler end');
    EXECUTE IMMEDIATE 'drop index afaiv.idx_tmp_client_Test';
  /*EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;*/
    
  END pro_esclientpositionhandler;

  PROCEDURE pro_esusermodularhadler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  ) IS
  BEGIN
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esusermodularhadler start');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.T_ES_USER_MODULAR_TMP';
    INSERT INTO afaiv.t_es_user_modular_tmp
      (user_id
      ,login_id
      ,modular_id
      ,modular_name
      ,modular_name_all
      ,modular_url
      ,operate_type
      ,batch_id
      ,elasticsearch_tag
      ,elasticsearch_tag_name
      ,create_time
      ,update_time)
      SELECT DISTINCT a.user_id
                     ,a.login_id
                     ,d.modular_id
                     ,d.modular_name
                     ,'tmp'
                     ,d.url
                     ,'1'
                     ,NULL
                     ,'user_modular_index'
                     ,'菜单'
                     ,SYSDATE
                     ,SYSDATE
        FROM t_xtgl_user a, t_xtgl_userrole b, t_xtgl_rolemodular c, t_xtgl_modular d, dual
       WHERE a.user_id = b.user_id
         AND b.role_id = c.role_id
         AND c.modular_id = d.modular_id
         AND a.rec_status = 1
         AND d.isvalid = '1';
  
    UPDATE afaiv.t_es_user_modular_tmp t SET t.elasticsearch_id = rownum;
    UPDATE afaiv.t_es_user_modular_tmp t SET t.batch_sub_id = ceil(rownum / 10000);
    UPDATE afaiv.t_es_user_modular_tmp t SET t.batch_id = MOD(t.batch_sub_id, 8) + 1;
    UPDATE afaiv.t_es_user_modular_tmp t SET elasticsearch_id = rownum, batch_sub_id = ceil(rownum / 10000);
    COMMIT;
  
    BEGIN
      FOR m IN (SELECT DISTINCT modular_id FROM afaer.t_xtgl_modular) LOOP
        UPDATE afaiv.t_es_user_modular_tmp t
           SET t.modular_name_all =
               (SELECT REPLACE(wm_concat(modular_name), ',', '-')
                  FROM (SELECT d.modular_name
                          FROM t_xtgl_modular d
                         START WITH d.modular_id = m.modular_id
                        CONNECT BY PRIOR parent_id = d.modular_id and d.modular_name <> '根节点'
                         ORDER BY LEVEL DESC))
         WHERE t.modular_id = m.modular_id;
        COMMIT;
      END LOOP;
    END;
    INSERT INTO afaiv.t_elastic_job_log (title, log_detail, insert_date)
           VALUES ('user_modular', '1. Oracle procedure completed', SYSDATE);
    COMMIT;
    --时间标记----
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esusermodularhadler end');
  
  END pro_esusermodularhadler;

  PROCEDURE pro_esuserhelphandler
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  ) IS
  BEGIN
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esuserhelphandler start');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE afaiv.T_ES_USER_HELP_TMP';
    INSERT INTO afaiv.t_es_user_help_tmp
      (user_id, login_id, modular_id, function_id, function_name, chapter_id, digest)
      SELECT DISTINCT a.user_id, a.login_id, g.modular_id, g.function_id, g.function_name, g.chapter_id, g.digest
        FROM t_xtgl_user a
            ,t_xtgl_userrole b
            ,t_xtgl_rolemodular c
            ,(SELECT d.chapter_id
                    ,d.modular_id
                    ,d.function_id
                    ,d.digest
                    ,(SELECT e.modular_name FROM t_xtgl_modular e WHERE e.modular_id = d.modular_id) modular_name
                    ,(SELECT f.function_name FROM t_xtgl_function f WHERE f.function_id = d.function_id) function_name
                FROM t_chapter_info d
               WHERE d.status = 1
                 AND d.dle_status = 1) g
       WHERE a.user_id = b.user_id
         AND b.role_id = c.role_id
         AND c.modular_id = g.modular_id;
  
    UPDATE afaiv.t_es_user_help_tmp t
       SET t.modular_name          =
           (SELECT DISTINCT modular_name FROM t_xtgl_modular d WHERE d.modular_id = t.modular_id)
          ,t.operate_type           = '1'
          ,t.elasticsearch_id       = rownum
          ,t.elasticsearch_tag      = 'user_modular_index'
          ,t.elasticsearch_tag_name = '菜单'
          ,t.create_time            = SYSDATE
          ,t.update_time            = SYSDATE
          ,t.batch_sub_id           = ceil(rownum / 10000);
  
    UPDATE afaiv.t_es_user_help_tmp t SET t.batch_id = MOD(t.batch_sub_id, 8);
    COMMIT;
    BEGIN
      FOR m IN (SELECT DISTINCT modular_id FROM afaer.t_xtgl_modular) LOOP
        UPDATE afaiv.t_es_user_help_tmp t
           SET t.modular_name_all =
               (SELECT REPLACE(wm_concat(modular_name), ',', '-')
                  FROM (SELECT d.modular_name
                          FROM t_xtgl_modular d
                         START WITH d.modular_id = m.modular_id
                        CONNECT BY PRIOR parent_id = d.modular_id and d.modular_name <> '根节点'
                         ORDER BY LEVEL DESC))
         WHERE t.modular_id = m.modular_id;
        COMMIT;
      END LOOP;
    END;
    INSERT INTO afaiv.t_elastic_job_log (title, log_detail, insert_date)
           VALUES ('user_help', '1. Oracle procedure completed', SYSDATE);
    COMMIT;
    --时间标记----
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   pro_esuserhelphandler end');
  
  END pro_esuserhelphandler;

  PROCEDURE pro_handle_afadiv
  (
    o_code OUT NUMBER
   ,o_note OUT VARCHAR2
  ) IS
  BEGIN
    pro_esclientbasehandler(o_code, o_note);
    pro_esclientpositionhandler(o_code, o_note);
    pro_esusermodularhadler(o_code, o_note);
    pro_esuserhelphandler(o_code, o_note);
    dbms_output.put_line(to_char(SYSDATE, 'yyyymmdd HH24:MI:SS') || '   end');
     
    UPDATE afaiv.t_elastic_job_task t
       SET t.process_status = 'ready', t.start_time = SYSDATE, t.update_time = SYSDATE
    WHERE t.title = 'client_base_index' AND t.job_action = 'C';
    commit;

  END pro_handle_afadiv;

END pkg_pro_handle_afadiv;
