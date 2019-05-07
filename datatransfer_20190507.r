setwd("~/R/ML/LIAB")
library('doParallel')
cl<-makeCluster(50) 
registerDoParallel(cl)


################################################################################

data2013 = read_csv('content2013.csv')
data2014 = read_csv('content2014.csv')
data2015 = read_csv('content2015.csv')
data2016 = read_csv('content2016.csv')
data2017 = read_csv('content2017.csv')
head(data2017)
str(data2017)

dataset = rbind(data2013,data2014,data2015,data2016,data2017)
head(dataset)

rm(data2013,data2014,data2015,data2016,data2017)
gc()

datatmp=dataset

################################################################################

drop_col = c('CONTRACT_NO',  'APPLLTE_NO',	'PRODUCT_CODE',	'PRODUCT_SUB_CODE',	'POLICY_NO',	'POLICY_STATUS',	'POLICY_RESULT_CODE',	'APPLLTE_DATE',	'ISSUE_DATE',	'END_DATETIME',	'COPERATE_COMPANY_NO',	'INSRNCE_TARGET',	'CONTINUE_POLICY_NO',	'AGENT_ID',	'AGENT_DIV_NO',	'COUNSEL_ID',	'COUNSEL_DIV_NO',	'CUSTOMER_CLASSFY',	'IDNTFCTN_TYPE',	'CUSTOMER_ID',	'IS_MAIN_INSURED',	'BIRTHDAY',	'OCCU_CATEGORY_CODE',	'OCCU_CATEGORY_CODE_IND',	'POLICY_TYPE',	'CAR_NO',	'ENGINE_NO',	'ORIGIN_ISSUE_DATE',	'MADE_DATE',	'LEGAL_PREMIUM',	'TRADE_FORCE_NO',	'TRADE_START_DATE',	'ACCEPT_OFFICER',	'VEHICLE_KIND_NAME',	'MODEL_SUB_NAME',	'MODEL_NAME',	'IS_WHITE_LIST',	'INSURE')
remove<- names(dataset)  %in% c(drop_col)
dataset<-dataset[!remove]
dim(dataset) 

# 轉換函數
label_encoder = function(vec){
  levels = sort(unique(vec))
  function(x){
    match(x, levels)
  }
}

# feature engineer
LE_PRODUCT_POLICY_CODE = label_encoder(dataset$PRODUCT_POLICY_CODE)
tmp_col = as.integer(LE_PRODUCT_POLICY_CODE(dataset$PRODUCT_POLICY_CODE))
dataset$PRODUCT_POLICY_CODE = tmp_col

LE_UNIT_NO = label_encoder(dataset$UNIT_NO)
tmp_col = as.integer(LE_UNIT_NO(dataset$UNIT_NO))
dataset$UNIT_NO = tmp_col

tmp_col = ifelse(dataset$BUSINESS_ORIGIN %in% c('XXX','XXZ','XXY','XXJ'),dataset$BUSINESS_ORIGIN,'OTHS')
LE_BUSINESS_ORIGIN = label_encoder(tmp_col)
tmp_col = as.integer(LE_BUSINESS_ORIGIN(tmp_col))
dataset$BUSINESS_ORIGIN = tmp_col

LE_CHANNEL_TYPE = label_encoder(dataset$CHANNEL_TYPE)
tmp_col = as.integer(LE_CHANNEL_TYPE(dataset$CHANNEL_TYPE))
dataset$CHANNEL_TYPE = tmp_col

tmp_col = as.integer(ifelse(dataset$IS_CONTINUE_POLICY_NO=='Y',1,0))
dataset$IS_CONTINUE_POLICY_NO = tmp_col

tmp_col = ifelse(dataset$BROKER_NO %in% c('X3',  'X1',	'X8',	'XB',	'HT',	'FW',	'KS',	'6A',	'FV',	'SH'),dataset$BROKER_NO,'OTHS')
LE_BROKER_NO = label_encoder(tmp_col)
tmp_col = as.integer(LE_BROKER_NO(tmp_col))
dataset$BROKER_NO = tmp_col

tmp_col = ifelse(dataset$AGENT_KIND %in% c('X3',  'X1',  'X8'),dataset$AGENT_KIND,'OTHS')
LE_AGENT_KIND = label_encoder(tmp_col)
tmp_col = as.integer(LE_AGENT_KIND(tmp_col))
dataset$AGENT_KIND = tmp_col

tmp_col = as.integer(ifelse(dataset$SEX=='M',1,ifelse(dataset$SEX=='F',0,NA)))
dataset$SEX = tmp_col

dataset = dataset[dataset$AGE<=90 & dataset$AGE>=17,]
dim(dataset)

LE_BUSINESS_TYPE = label_encoder(dataset$BUSINESS_TYPE)
tmp_col = as.integer(LE_BUSINESS_TYPE(dataset$BUSINESS_TYPE))
dataset$BUSINESS_TYPE = tmp_col

LE_OCCU_TYPE1_CODE = label_encoder(dataset$OCCU_TYPE1_CODE)
tmp_col = as.integer(LE_OCCU_TYPE1_CODE(dataset$OCCU_TYPE1_CODE))
dataset$OCCU_TYPE1_CODE = tmp_col

dataset = dataset[dataset$ORIGIN_ISSUE_DUR>0 & dataset$ORIGIN_ISSUE_DUR<=70,]
dim(dataset)

dataset = dataset[dataset$CAR_AGE<=70,]
dim(dataset) #8084236 * 122

tmp_col = ifelse(dataset$MODEL_FULL_NO %in% c('MC000000',  'MB000000',  'MA000000',	'09000000',	'07000000',	'01000000',	'03000000',	'04000000',	'MD000000',	'11000000'),dataset$MODEL_FULL_NO,'OTHS')
LE_MODEL_FULL_NO = label_encoder(tmp_col)
tmp_col = as.integer(LE_MODEL_FULL_NO(tmp_col))
dataset$MODEL_FULL_NO = tmp_col

tmp_col = ifelse(dataset$VEHICLE_KIND_NO %in% c('01',  '03',  '22',	'02',	'04',	'15',	'07',	'19',	'06',	'34'),dataset$VEHICLE_KIND_NO,'OTHS')
LE_VEHICLE_KIND_NO = label_encoder(tmp_col)
tmp_col = as.integer(LE_VEHICLE_KIND_NO(tmp_col))
dataset$VEHICLE_KIND_NO = tmp_col

tmp_col = ifelse(dataset$KIND_NO %in% c('普通重型機器腳踏車',  '自用小客車',  '客貨兩用車',  '輕型機器腳踏車',	'普通重機',	'自用小貨車',	'自小客車',	'客貨兩用',	'自小貨車',	'個人計程車',	'輕型機車',	'公司行號自用小貨車',	'營業小客車'),dataset$KIND_NO,'OTHS')
LE_KIND_NO = label_encoder(tmp_col)
tmp_col = as.integer(LE_KIND_NO(tmp_col))
dataset$KIND_NO = tmp_col

tmp_col = dataset$ENGINE_EXHAUST
tmp_col = ifelse(tmp_col<=124,1,ifelse(tmp_col>=124 & tmp_col<150,2,ifelse(tmp_col>=150 & tmp_col<500,3,ifelse(tmp_col>=500&tmp_col<1000,4,ifelse(tmp_col>=1000 & tmp_col<2000,5,ifelse(tmp_col>=2000 & tmp_col<5000,6,ifelse(tmp_col>=5000 & tmp_col<10000,7,8)))))))
dataset$ENGINE_EXHAUST = tmp_col

LE_ENGINE_EXHAUST_UNIT = label_encoder(dataset$ENGINE_EXHAUST_UNIT)
tmp_col = as.integer(LE_ENGINE_EXHAUST_UNIT(dataset$ENGINE_EXHAUST_UNIT))
dataset$ENGINE_EXHAUST_UNIT = tmp_col

dataset = dataset[dataset$MAXIMUM_LOAD_UNIT!='T',]
dim(dataset) #8084236 * 122
dataset$MAXIMUM_LOAD_UNIT=NULL

tmp_col = dataset$MAXIMUM_LOAD
tmp_col = ifelse(tmp_col>20,21,tmp_col)
dataset$MAXIMUM_LOAD = tmp_col

tmp_col = dataset$REPLACE_PRICE
tmp_col = ifelse(tmp_col==0,1,ifelse(tmp_col>0 & tmp_col<=100000,2,ifelse(tmp_col>100000 & tmp_col<=500000,3,ifelse(tmp_col>500000 & tmp_col<=700000,4,ifelse(tmp_col>700000 & tmp_col<=900000,5,ifelse(tmp_col>900000,6,7))))))
dataset$REPLACE_PRICE = tmp_col

tmp_col = dataset$BODY_PREMIUM_RATE
tmp_col = ifelse(tmp_col==0,0,ifelse(tmp_col>0 & tmp_col<0.5,1,ifelse(tmp_col>=0.5 & tmp_col<1,2,ifelse(tmp_col>=1 & tmp_col<1.5,3,ifelse(tmp_col>=1.5 & tmp_col<2,4,ifelse(tmp_col>=2 & tmp_col<2.5,5,ifelse(tmp_col>=2.5 & tmp_col<3,6,7)))))))
dataset$BODY_PREMIUM_RATE = tmp_col

tmp_col = dataset$LCNR_PREMIUM_RATE
tmp_col = ifelse(tmp_col<0,-1,ifelse(tmp_col==0,0,ifelse(tmp_col>0 & tmp_col<1,1,ifelse(tmp_col>=1 & tmp_col<2,2,3))))
dataset$LCNR_PREMIUM_RATE = tmp_col

LE_AGE_RATE = label_encoder(dataset$AGE_RATE)
tmp_col = as.integer(LE_AGE_RATE(dataset$AGE_RATE))
dataset$AGE_RATE = tmp_col

tmp_col = log(dataset$BODY_CLAIM_TIMES1+1)
dataset$BODY_CLAIM_TIMES1 = tmp_col

tmp_col = log(dataset$BODY_CLAIM_TIMES2+1)
dataset$BODY_CLAIM_TIMES2 = tmp_col

tmp_col = log(dataset$BODY_CLAIM_TIMES3+1)
dataset$BODY_CLAIM_TIMES3 = tmp_col

LE_BODY_CLAIM_RATE = label_encoder(dataset$BODY_CLAIM_RATE)
tmp_col = as.integer(LE_BODY_CLAIM_RATE(dataset$BODY_CLAIM_RATE))
dataset$BODY_CLAIM_RATE = tmp_col

tmp_col = log(dataset$LIAB_CLAIM_TIMES1+1)
dataset$LIAB_CLAIM_TIMES1 = tmp_col

tmp_col = log(dataset$LIAB_CLAIM_TIMES2+1)
dataset$LIAB_CLAIM_TIMES2 = tmp_col

tmp_col = log(dataset$LIAB_CLAIM_TIMES3+1)
dataset$LIAB_CLAIM_TIMES3 = tmp_col

LE_LIAB_CLAIM_RATE = label_encoder(dataset$LIAB_CLAIM_RATE)
tmp_col = as.integer(LE_LIAB_CLAIM_RATE(dataset$LIAB_CLAIM_RATE))
dataset$LIAB_CLAIM_RATE = tmp_col

tmp_col = dataset$COMPUTER_CALCULAT_CODE
tmp_col = ifelse(tmp_col %in% c('1','y','Y'),1,0)
dataset$COMPUTER_CALCULAT_CODE = tmp_col

LE_SHORT_PREMIUM_RATE = label_encoder(dataset$SHORT_PREMIUM_RATE)
tmp_col = as.integer(LE_SHORT_PREMIUM_RATE(dataset$SHORT_PREMIUM_RATE))
dataset$SHORT_PREMIUM_RATE = tmp_col

LE_DEPRECIA_RATE = label_encoder(dataset$DEPRECIA_RATE)
tmp_col = as.integer(LE_DEPRECIA_RATE(dataset$DEPRECIA_RATE))
dataset$DEPRECIA_RATE = tmp_col

LE_TAXI_PRE_FACTOR = label_encoder(dataset$TAXI_PRE_FACTOR)
tmp_col = as.integer(LE_TAXI_PRE_FACTOR(dataset$TAXI_PRE_FACTOR))
dataset$TAXI_PRE_FACTOR = tmp_col

tmp_col = dataset$IS_NEW_CAR
tmp_col = ifelse(tmp_col  =='Y',1,0)
dataset$IS_NEW_CAR = tmp_col

tmp_col = dataset$SUM_INSRNCE_AMOUNT
tmp_col = cut(tmp_col,c(-Inf,seq(5,200,10)*1000000,Inf),labels=F) 
dataset$SUM_INSRNCE_AMOUNT = tmp_col

tmp_col = dataset$SUM_DEATH_AMOUNT
tmp_col = cut(tmp_col,c(-Inf,0,seq(5,50,10)*1000000,Inf),labels=F) 
dataset$SUM_DEATH_AMOUNT = tmp_col

tmp_col = dataset$SUM_INJURY_AMOUNT
tmp_col = cut(tmp_col,c(-Inf,0,seq(5,20,5)*1000000,Inf),labels=F) 
dataset$SUM_INJURY_AMOUNT = tmp_col

tmp_col = dataset$SUM_EXTRA_AMOUNT
tmp_col = cut(tmp_col,c(-Inf,0,Inf),labels=F) 
dataset$SUM_EXTRA_AMOUNT = tmp_col

tmp_col = dataset$FLG_31
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_31 = tmp_col

tmp_col = dataset$FLG_32
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_32 = tmp_col

tmp_col = dataset$FLG_3A
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_3A = tmp_col

tmp_col = dataset$FLG_2A
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_2A = tmp_col

tmp_col = dataset$FLG_5B
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_5B = tmp_col

tmp_col = dataset$FLG_24
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FLG_24 = tmp_col

tmp_col = dataset$QUESTION_CAR_NO_IND
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$QUESTION_CAR_NO_IND = tmp_col

tmp_col = dataset$QUESTION_ENGIN_IND
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$QUESTION_ENGIN_IND = tmp_col

tmp_col = dataset$USE_TYPE
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$USE_TYPE = tmp_col

tmp_col = dataset$IS_NOT_FORCE
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_NOT_FORCE = tmp_col

tmp_col = ifelse(dataset$MODEL_NO %in% c('MC',  'MB',  '09',	'MA',	'07',	'01',	'03',	'04',	'C8',	'11'),dataset$MODEL_NO,'OTHS')
LE_MODEL_NO = label_encoder(tmp_col)
tmp_col = as.integer(LE_MODEL_NO(tmp_col))
dataset$MODEL_NO = tmp_col

tmp_col = dataset$MOTOR_FLG
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$MOTOR_FLG = tmp_col

tmp_col = dataset$RISK_SCORE
tmp_col = cut(tmp_col,c(-Inf,seq(0,1,0.1),Inf),labels=F) 
dataset$RISK_SCORE = tmp_col

tmp_col = dataset$SUSPECT_LEVEL
tmp_col = ifelse(tmp_col  =='L',1,tmp_col)
tmp_col = ifelse(tmp_col  =='M',2,tmp_col)
tmp_col = ifelse(tmp_col  =='H',3,tmp_col)
tmp_col = ifelse(tmp_col  =='X',4,tmp_col)
dataset$SUSPECT_LEVEL = tmp_col

tmp_col = dataset$IS_AGENT
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_AGENT = tmp_col

tmp_col = dataset$IS_RAGENT
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_RAGENT = tmp_col

tmp_col = dataset$IS_MAGENT
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_MAGENT = tmp_col

tmp_col = dataset$IS_DIRECT
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_DIRECT = tmp_col

tmp_col = dataset$IS_OTHER_CH
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_OTHER_CH = tmp_col

tmp_col = dataset$IS_WEB_CH
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_WEB_CH = tmp_col

tmp_col = dataset$ONE_INSUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$ONE_INSUR = tmp_col

tmp_col = dataset$TWO_INSUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$TWO_INSUR = tmp_col

tmp_col = dataset$THREE_INSUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$THREE_INSUR = tmp_col

tmp_col = dataset$FOUR_INSUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FOUR_INSUR = tmp_col

tmp_col = dataset$FIVE_INSUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$FIVE_INSUR = tmp_col

tmp_col = dataset$IS_CONTINUE_TWO
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_CONTINUE_TWO = tmp_col

tmp_col = dataset$IS_CONTINUE_THREE
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_CONTINUE_THREE = tmp_col

tmp_col = dataset$IS_CONTINUE_FOUR
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_CONTINUE_FOUR = tmp_col

tmp_col = dataset$IS_CONTINUE_FIVE
tmp_col = ifelse(tmp_col=='Y',1,0)
dataset$IS_CONTINUE_FIVE = tmp_col

tmp_col = dataset$LOSS_RATE
tmp_col = ifelse(tmp_col>0,1,0)
dataset$LOSS_RATE_IND = tmp_col

#############################################################################
write.table(dataset,'dataset.csv',sep=',', row.names=F)