# output used in script F:\Xalt\Spectrum\TC1 Migration\Deployment\Step 1 - insert into spectrum enabled cdc table.sql
# this script inserts the table names into a table in xalt_prod that can be used to create the states table for Step 3




table_names = [
"CR_CHANGE_ORDER_HEADER_MC","CR_CHANGE_ORDER_STATUS_MC","CR_CHNG_ORD_REV_MC","CR_CHNG_REQ_ADDON_MC","CR_CHNG_REQ_CON_DET_MC","CR_CHNG_REQ_HDR_MC","CR_CHNG_REQ_MARKUP_MC","CR_CHNG_REQ_REV_MC","CR_CHNG_REQ_STATUS_MC","CR_CONTRACT_MARKUP_MC"
,"CR_CONTRACT_MASTER_MC","CR_CONTRACT_PRICING_ADDON_MC","CR_CUST_USER_DEF_FIELDS_MC","CR_CUSTOMER_MASTER_MC","CR_DRAW_REQUEST_DETAIL_MC","CR_DRAW_UNIT_PRICE_BILL_MC","CR_INVOICE_DETAIL_MC","CR_INVOICE_HEADER_MC","CR_INVOICE_LOG_MC"
,"CR_INVOICE_SPECIFIC_NOTES_MC","CR_OPEN_ITEM_MC","CR_PAY_ADJUST_HISTORY_MC","CR_PRICING_ADDON_MC","CR_SALES_TAX_MASTER_MC","CR_TERMS_MASTER_MC","CR_UNIT_OF_MEASURE_MC","DI_IMAGE_MASTER","DI_MASTER_MC","EC_EQUIPMENT_MASTER_MC"
,"EC_EQUIPMENT_STATUS_MC","EC_JOB_EQUIPMENT_RATE_MC","EC_YARD_MAINTENANCE_MC","EM_COST_CENTERS_MC","EM_ENTITIES_MC","EM_ENTITY_TAX_XREF_MC","ET_EQUIPMENT_ISSUED_MC","ET_REQ_HISTORY_DETAIL_MC","ET_REQ_HISTORY_HEADER_MC","GL_BALANCE_MC"
,"GL_BUDGET_MC","GL_DEPARTMENT_MC","GL_DETAIL_MC","GL_FISCAL_CALENDAR_DETAIL","GL_MASTER_MC","HR_EMP_DEPENDENTS_MC","IM_ITEM_MASTER_MC","IM_REQUISITION_DETAIL_MC","IM_REQUISITION_HEADER_MC","IM_TRANSACTION_HISTORY_MC","JC_ALT_BILLING_MC"
,"JC_BAL_YR_PER_RANGE_MC","JC_CM_COMMITTED_H_MC","JC_COST_TYPE_MASTER_MC","JC_EARNED_REVENUE_HISTORY_MC","JC_JOB_CONTACTS_MC","JC_JOB_MASTER_MC","JC_JOB_USER_FIELDS_DET_MC","JC_PHASE_ESTIMATES_MC","JC_PHASE_MASTER_MC","JC_PROJ_COST_HISTORY_MC"
,"JC_QUANTITY_HISTORY_MC","JC_STD_PHASE_DESCRIPTION_MC","JC_TRANSACTION_HISTORY_MC","MC_CURRENCY_EXCHANGE_RATES_MC","MC_CURRENCY_MASTER_MC","PA_ADDRESS_MASTER","PA_COMPANY_INFORMATION","PA_CONTACTS_MASTER","PA_CONTROL_FILE_MC"
,"PA_GLOBAL_BUSINESS_UNIT_MAP","PA_GLOBAL_ENTITY_MAP","PA_GLOBAL_USER_MAP","PA_LOCAL_COMPANY_MAP","PA_NOTES_MC","PA_OPERATOR_FILE_MC","PA_OPERATOR_MASTER","PA_PHONE_MASTER","PA_USER_FIELDS_SETUP_U_MC","PA_VALUE_VARIABLES"
,"PJ_CORRESPONDENCE_HEADER_MC","PO_BLANKET_RELEASE_HEADER_MC","PO_PURCHASE_ORDER_DETAIL_MC","PO_PURCHASE_ORDER_HEADER_MC","PR_BASE_WORK_COMP_MC","PR_CHECK_HISTORY_MC","PR_DEPARTMENT_MC","PR_EMPL_VOL_DEDUCT_MC","PR_EMPLOYEE_ENTITY_MASTER_2_MC"
,"PR_EMPLOYEE_LAST_ENTRIES_MC","PR_EMPLOYEE_MASTER_1_MC","PR_EMPLOYEE_MASTER_2_MC","PR_EMPLOYEE_MASTER_3_MC","PR_EMPLOYEE_STATUS_MC","PR_INCOME_TAX_TABLE_MC","PR_PRE_TIME_CARD_MC","PR_RATE_REVISION_MC","PR_TIME_CARD_DETAIL_MC"
,"PR_TIME_CARD_HISTORY_MC","PR_TIME_OFF_ANNIVERSARY_MC","PR_TIME_OFF_BANK_LOG_MC","PR_UNION_MASTER_1_MC","PR_VAC_HOL_SICK_MASTER_MC","PR_W2_HISTORY_MC","PR_WAGE_CLASS_MASTER_MC","PR_WORK_CLASS_MC","SC_CONTRACT_MC","SC_CONTRACT_USER_FIELDS_MC"
,"TM_JOB_MARKUP_MC","TM_LABOR_BILLING_RATES_MC","VN_CHNG_REQ_SUB_DET_MC","VN_CHNG_REQ_SUB_HDR_MC","VN_GL_DISTRIBUTION_DETAIL_MC","VN_GL_DISTRIBUTION_HEADER_MC","VN_SUBCONTRACT_MC","VN_SUBCONTRACT_PHASE_MC","VN_VENDOR_CURRENCY_OVERRIDE_MC"
,"VN_VENDOR_LOCATIONS_MC","VN_VENDOR_MASTER_MC","WO_ADDRESS_MC","WO_BILLING_HISTORY_MC","WO_CASE_TYPE_MC","WO_COMPONENT_MC","WO_COST_HISTORY_MC","WO_DISPATCH_STATUS_MC","WO_EMPL_HOURS_HIST_DETAIL_MC","WO_EMPL_LABOR_HOURS_DET_MC"
,"WO_EMPLOYEE_CATEGORY_MC","WO_HEADER_MC","WO_PRIORITY_MC","WO_TYPE_MC","WO_WO_USER_FIELDS_DET_MC","WO_ZONE_MC"

]

query = " UNION ALL\n".join([f"SELECT '{table_name}'" for table_name in table_names])

print(query)
