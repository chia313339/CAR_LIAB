rm(list = ls())
gc()

setwd("~/R/ML/CAR")
# library('doParallel')
# cl<-makeCluster(50) 
# registerDoParallel(cl)



# rm(list = ls())
# gc()
# data2012 = read_csv('dataset2012.csv')
# data2013 = read_csv('dataset2013.csv')
# data2014 = read_csv('dataset2014.csv')
# data2015 = read_csv('dataset2015.csv')
# data2016 = read_csv('dataset2016.csv')
# data2017 = read_csv('dataset2017.csv')
# 
# 
# dataset = rbind(data2012,data2013,data2014,data2015,data2016,data2017)
# 
# rm(data2012,data2013,data2014,data2015,data2016,data2017)
# gc()

# saveRDS(dataset,'dataset_carall.rds')

dataset = readRDS('~/../public/LIAB/dataset_all_preprocess_20190517.rds')

dataset = as.data.frame(dataset)

################################################################################

# dataset$START_DATETIME = as.Date(dataset$START_DATETIME)
# dataset$END_DATETIME = as.Date(dataset$END_DATETIME)

drop_list =c('APPLLTE_NO',  'PRODUCT_CODE', 'PRODUCT_POLICY_CODE',	'PRODUCT_SUB_CODE',	'POLICY_NO', 'POLICY_STATUS',	'POLICY_RESULT_CODE',	'APPLLTE_DATE', 'ISSUE_DATE',	'END_DATETIME',	'INSRNCE_TARGET', 'CONTINUE_POLICY_NO',	'AGENT_ID',	'COUNSEL_ID', 'CUSTOMER_CLASSFY',	'CUSTOMER_ID',	'IS_MAIN_INSURED', 'BIRTHDAY',	'OCCU_CATEGORY_CODE', 'OCCU_CATEGORY_CODE_IND',	'POLICY_TYPE',	'CAR_NO', 'ENGINE_NO',	'ORIGIN_ISSUE_DATE',	'MADE_DATE', 'LEGAL_PREMIUM',	'TRADE_FORCE_NO',	'TRADE_START_DATE', 'ACCEPT_OFFICER',	'VEHICLE_KIND_NAME',	'MODEL_NAME', 'IS_WHITE_LIST')
drop_ind =  names(dataset)  %in% drop_list
dataset = dataset[!drop_ind]


rm(dataset2017,dataset2016,dataset2015,dataset2014,dataset2013,dataset2012)

################################################################################


# 轉換函數
label_encoder = function(vec){
  levels = sort(unique(vec))
  function(x){
    match(x, levels)
  }
}

# feature engineer
LE_UNIT_NO = label_encoder(dataset$UNIT_NO)
tmp_col = as.integer(LE_UNIT_NO(dataset$UNIT_NO))
dataset$UNIT_NO = tmp_col

tmp_col = ifelse(dataset$BUSINESS_ORIGIN %in% c('XXX','XXZ','XXY','XXJ'),dataset$BUSINESS_ORIGIN,'OTHS')
LE_BUSINESS_ORIGIN = label_encoder(tmp_col)
tmp_col = as.integer(LE_BUSINESS_ORIGIN(tmp_col))
dataset$BUSINESS_ORIGIN = tmp_col

LE_COPERATE_COMPANY_NO = label_encoder(dataset$COPERATE_COMPANY_NO)
tmp_col = as.integer(LE_COPERATE_COMPANY_NO(dataset$COPERATE_COMPANY_NO))
dataset$COPERATE_COMPANY_NO = tmp_col

LE_AGENT_DIV_NO = label_encoder(dataset$AGENT_DIV_NO)
tmp_col = as.integer(LE_AGENT_DIV_NO(dataset$AGENT_DIV_NO))
dataset$AGENT_DIV_NO = tmp_col

LE_COUNSEL_DIV_NO = label_encoder(dataset$COUNSEL_DIV_NO)
tmp_col = as.integer(LE_COUNSEL_DIV_NO(dataset$COUNSEL_DIV_NO))
dataset$COUNSEL_DIV_NO = tmp_col

LE_BROKER_NO = label_encoder(dataset$BROKER_NO)
tmp_col = as.integer(LE_BROKER_NO(dataset$BROKER_NO))
dataset$BROKER_NO = tmp_col

LE_AGENT_KIND = label_encoder(dataset$AGENT_KIND)
tmp_col = as.integer(LE_AGENT_KIND(dataset$AGENT_KIND))
dataset$AGENT_KIND = tmp_col

tmp_col = as.integer(ifelse(dataset$SEX=='M',1,ifelse(dataset$SEX=='F',0,NA)))
dataset$SEX = tmp_col

LE_BUSINESS_TYPE = label_encoder(dataset$BUSINESS_TYPE)
tmp_col = as.integer(LE_BUSINESS_TYPE(dataset$BUSINESS_TYPE))
dataset$BUSINESS_TYPE = tmp_col

LE_OCCU_TYPE1_CODE = label_encoder(dataset$OCCU_TYPE1_CODE)
tmp_col = as.integer(LE_OCCU_TYPE1_CODE(dataset$OCCU_TYPE1_CODE))
dataset$OCCU_TYPE1_CODE = tmp_col

LE_MODEL_FULL_NO = label_encoder(dataset$MODEL_FULL_NO)
tmp_col = as.integer(LE_MODEL_FULL_NO(dataset$MODEL_FULL_NO))
dataset$MODEL_FULL_NO = tmp_col

LE_VEHICLE_KIND_NO = label_encoder(dataset$VEHICLE_KIND_NO)
tmp_col = as.integer(LE_VEHICLE_KIND_NO(dataset$VEHICLE_KIND_NO))
dataset$VEHICLE_KIND_NO = tmp_col

LE_KIND_NO = label_encoder(dataset$KIND_NO)
tmp_col = as.integer(LE_KIND_NO(dataset$KIND_NO))
dataset$KIND_NO = tmp_col

LE_BODY_NO = label_encoder(dataset$BODY_NO)
tmp_col = as.integer(LE_BODY_NO(dataset$BODY_NO))
dataset$BODY_NO = tmp_col

LE_MODEL_SUB_NAME = label_encoder(dataset$MODEL_SUB_NAME)
tmp_col = as.integer(LE_MODEL_SUB_NAME(dataset$MODEL_SUB_NAME))
dataset$MODEL_SUB_NAME = tmp_col

LE_MODEL_NO = label_encoder(dataset$MODEL_NO)
tmp_col = as.integer(LE_MODEL_NO(dataset$MODEL_NO))
dataset$MODEL_NO = tmp_col

LE_RISK_LEVEL = label_encoder(dataset$RISK_LEVEL)
tmp_col = as.integer(LE_RISK_LEVEL(dataset$RISK_LEVEL))
dataset$RISK_LEVEL = tmp_col

LE_SUSPECT_LEVEL = label_encoder(dataset$SUSPECT_LEVEL)
tmp_col = as.integer(LE_SUSPECT_LEVEL(dataset$SUSPECT_LEVEL))
dataset$SUSPECT_LEVEL = tmp_col

tmp_col = as.integer(ifelse(is.na(dataset$COMPUTER_CALCULAT_CODE),0,1))
dataset$COMPUTER_CALCULAT_CODE = tmp_col

tmp_col = as.integer(ifelse(dataset$IS_CONTINUE_POLICY_NO=='N',0,1))
dataset$IS_CONTINUE_POLICY_NO = tmp_col

LE_MAXIMUM_LOAD_UNIT = label_encoder(dataset$MAXIMUM_LOAD_UNIT)
tmp_col = as.integer(LE_MAXIMUM_LOAD_UNIT(dataset$MAXIMUM_LOAD_UNIT))
dataset$MAXIMUM_LOAD_UNIT = tmp_col

dataset$AUTO_HAND = as.integer(dataset$AUTO_HAND)

tmp_col = as.integer(ifelse(dataset$IS_NEW_CAR=='N',0,1))
dataset$IS_NEW_CAR = tmp_col

tmp_col = as.integer(ifelse(dataset$QUESTION_CAR_NO_IND=='N',0,1))
dataset$QUESTION_CAR_NO_IND = tmp_col

tmp_col = as.integer(ifelse(dataset$QUESTION_ENGIN_IND=='N',0,1))
dataset$QUESTION_ENGIN_IND = tmp_col

tmp_col = as.integer(ifelse(dataset$IS_NOT_FORCE=='N',0,1))
dataset$IS_NOT_FORCE = tmp_col

tmp_col = as.integer(ifelse(dataset$MOTOR_FLG=='N',0,1))
dataset$MOTOR_FLG = tmp_col

dataset$FORCE_INJURE_IND_L3Y = as.integer(dataset$FORCE_INJURE_IND_L3Y)

# remove strange data
dataset = dataset[dataset$ORIGIN_ISSUE_DUR>=0,]
dataset = dataset[dataset$AGE>=0,]
dataset = dataset[dataset$SPECIAL_INSRNCE_TYPE!='N',]


traindata = dataset[dataset$START_DATETIME>='2017-01-01',]
testdata = dataset[dataset$START_DATETIME<'2017-01-01',]

saveRDS(traindata,'traindata.rds')
saveRDS(testdata,'testdata.rds')






