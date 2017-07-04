package com.lakala.finance.stream.screen.biz;

/**
 * Created by longxiaolei on 2017/2/22.
 */
public interface Constant {
    interface Redis {
        //字段超时时间32天
        int EXPIRE = 2764800;

        //大屏对应的key前缀
        String PREFIX = "dataPlatform.";

        //每日统计相关redis的后缀
        String DAILY_COUNT_SUFFIX = "totalOneDay";
        String DAILY_AMOUNT_SUFFIX = "amountOneDay";

        //每五分钟区间
        String SUBSECTION_COUNT_SUFFIX = "totalTimeSubsection";
        String SUBSECTION_AMOUNT_SUFFIX = "amountTimeSubsection";

        //省市统计的后缀
        String CITY_DAILY_COUNT = "totalCityOneDay";
        String PROVINCE_DAILY_COUNT = "totalProvinceOneDay";

        //水位指针
        String WATER_POINTER = "dataPlatform.water_pointer";

        //每日统计的水位key
        String DAILY_COLLECTION_TIME_POINT = "DailyCollectionTimePoint";
        //每日统计备份逻辑的锁key
        String DAILY_CLEAN_LOCK = "DailyCleanLock";

        //地区统计的水位
        String LOCATION_COLLECTION_TIME_POINT = "LocationCollectionTimePoint";
        String LOCATION_CLEAN_LOCK = "LocationCleanLock";

        //每五分钟统计的水位
        String SUB_COLLECTION_TIME_POINT = "SubCollectionTimePoint";
        String SUB_CLEAN_LOCK = "SubCleanLock";

        String FUND_COLLECTION_TIME_POINT = "fundTimePoint";
        String FUND_CLEAN_LOCK = "fundCleanLock";

        String FINANCING_INFIX = "bamdata";
        //理财活期笔数
        String FUND_COUNT = "financingCurrentCount";
        //理财活期金额
        String FUND_AMOUNT = "financingCurrentAmount";
        //理财活期金额
        String FINANCING_COUNT = "financingRegularCount";
        //理财定期金额
        String FINANCING_AMOUNT = "financingRegularAmount";
        //理财总笔数
        String FUND_FINANCING_TOTAL_COUNT = "financingTotalCount";
        // 理财总金额
        String FUND_FINANCING_TOTAL_AMOUNT = "financingTotalAmount";

    }

    interface Flag {
        String EX = "Exception";

        String DAILY_APPLY_COUNT = "daily_apply_count";
        String DAILY_APPLY_AMOUNT = "daily_apply_amount";

        String DAILY_LOAN_COUNT = "daily_loan_count";
        String DAILY_LOAN_AMOUNT = "daily_loan_amount";

        String CITY_APPLY_COUNT = "city_apply_count";
        String PROVINCE_APPLY_COUNT = "province_apply_count";

        String SUB_APPLY_COUNT = "sub_apply_count";
        String SUB_APPLY_AMOUNT = "sub_apply_amount";
        String SUB_LOAN_COUNT = "sub_loan_count";
        String SUB_LOAN_AMOUNT = "sub_loan_amount";

        String FUND_COUNT = "fund_count";
        String FUND_AMOUNT = "fund_amount";

        String FINACING_COUNT = "Finacing_count";
        String FINACING_AMOUNT = "Finacing_amount";

        String FUND_FINACING_TOTAL_COUNT = "fund_finacing_total_count";
        String FUND_FINACING_TOTAL_AMOUNT = "fund_finacing_total_amount";

        //风控拒绝策略
        String RISK_REJECT_REASON = "RISK_REJECT_REASON";

        //风控等级
        String RISK_LEVEL = "risk_level";

        //风控结果
        String RISK_DECISION_RESULT = "risk_decision_result";


        //风控评分
        String RISK_CREDIT_SCORE = "risk_credit_score";

        //风控城市和省份
        String RISK_CITY_COUNT = "risk_city_count";
        String RISK_PROVINCE_COUNT = "risk_province_count";

        //风控：目标客户
        String RISK_CUSTOMER_TYPE_COUNT = "risk_customer_type_count";

        //同盾分数计数
        String RISK_K_SCORE = "risk_k_score";

        String RISK_CHANNEL = "risk_channel";

        //人工审批拒绝原因申请量
        String RISK_REJECT_CODE = "risk_reject_code";

        //(人工和系统)审批拒绝量和通过量
        String RISK_SYSTEM_APPROVE = "risk_system_approve";
        String RISK_MANUAL_APPROVE = "risk_manual_approve";

        //客户分布
        String RISK_CUSTOMER_DISTRIBUTION = "RISK_CUSTOMER_DISTRIBUTION";
    }

    interface LogNo {
        String APPLY_FLAG = "generalApply_CreditLoan";
        String LOAN_FLAG = "CreditLoan_WorkFlow_LoanAmount";

        String FUND_FLAG = "Fund_Market_Subscribe";
        String FINACING_FLAG = "Finacing_Transaction_Subscribe";

//        String PROFIT_RETURN = "Finacing_Transaction_ProfitReturn";
//        String MANUAL_AUDIT = "CreditLoan_WorkFlow_ManualAudit";

        String RISK_CONTROL = "RiskControl_SP_Result";

        String AUDIT_RESULT = "CreditLoan_WorkFlow_AuditResult";

    }

    interface SQL {
        //错误信息入库
        String ERRORINFO_2_DB_SQL = "INSERT INTO STREAMING_ERROR_INFO(COLLECTTIME,TASK_NAME,EXCEPTION_INFO,JSON) VALUES(?,?,?,?)";

        //查询每日进件数量
        String QUERY_APPLY = "SELECT T.APPLYAMT,T.COLLECTTIMEFMT,T.BELONGPROVINCE,T.BELONGCITY FROM S_DATA_CREDITLOAN_GENERALAPPLY_SHOW T WHERE T.COLLECTTIMEFMT >= ? AND T.COLLECTTIMEFMT < ?";
        //查询每日贷款的数量
        String QUERY_LOAN = "SELECT T.CAPITALAMOUNT,T.COLLECTTIME,NULL,NULL FROM S_DATA_CREDITLOAN_WORKFLOW_LOANAMOUNT T WHERE  T.COLLECTTIME >= ? AND T.COLLECTTIME < ?";

        //查询进件当天总计
        String QUERY_APPLY_DAILY = "SELECT COUNT(1) AS COUNT,SUM(APPLYAMT),SUBSTR(T.COLLECTTIME,1,10) AS DAYSTR FROM S_DATA_CREDITLOAN_GENERALAPPLY_SHOW T WHERE T.COLLECTTIME >=? AND  T.COLLECTTIMEFMT < ? GROUP BY DAYSTR ";

        //查询每日贷款总计
        String QUERY_LOAN_DAILY = "SELECT COUNT(1) AS COUNT,SUM(APPLYAMT),SUBSTR(T.COLLECTTIME,1,10) AS DAYSTR FROM S_DATA_CREDITLOAN_WORKFLOW_LOANAMOUNT T WHERE T.COLLECTTIME >=? AND  T.COLLECTTIMEFMT < ? GROUP BY DAYSTR";

        //查询每日的基金的数量及金额
        String QUERY_FUND = "SELECT COUNT(1) AS COUNT,SUM(T.AMOUNT) AS AMOUNT,SUBSTR(T.COLLECTTIME,1,10) AS DAYSTR FROM S_DATA_FUND_MARKET_SUBSCRIBE T   WHERE T.BUSID='1G2' AND T.COLLECTTIME >= ? AND T.COLLECTTIME < ? GROUP BY DAYSTR";
        //查询每日理财的数量及金额
        String QUERY_FINACING = "SELECT COUNT(1) AS COUNT,SUM(T.AMOUNT) AS AMOUNT,SUBSTR(T.COLLECTTIME,1,10) AS DAYSTR FROM S_DATA_FINACING_TRANSACTION_SUBSCRIBE T   WHERE  T.COLLECTTIME >= ? AND T.COLLECTTIME < ? GROUP BY DAYSTR";
    }
}
