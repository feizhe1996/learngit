
# coding: utf-8

# In[91]:


import pandas as pd


# In[92]:


basic_test = pd.read_csv("contest_basic_test.tsv",sep='\t')#基础表-测试集
basic_train = pd.read_csv("contest_basic_train.tsv",sep='\t')#基础表-训练集
crd_cd_ln = pd.read_csv("contest_ext_crd_cd_ln.tsv",sep='\t')#机构版征信-贷款
crd_cd_ln_spl = pd.read_csv("contest_ext_crd_cd_ln_spl.tsv",sep='\t')#机构版征信-贷款特殊交易
crd_cd_lnd = pd.read_csv("contest_ext_crd_cd_lnd.tsv",sep = '\t')#机构版征信-贷记卡
crd_cd_lnd_ovd = pd.read_csv("contest_ext_crd_cd_lnd_ovd.csv")#机构版征信-贷记卡逾期/透支记录
crd_hd_report = pd.read_csv("contest_ext_crd_hd_report.csv")#机构版征信-报告主表
crd_is_creditcue = pd.read_csv("contest_ext_crd_is_creditcue.csv")#机构版征信-信用提示
crd_is_ovdsummary = pd.read_csv("contest_ext_crd_is_ovdsummary.csv")#机构版征信-逾期(透支)信息汇总
crd_is_sharedebt = pd.read_csv("contest_ext_crd_is_sharedebt.csv")#机构版征信-未销户贷记卡或者未结清贷款
crd_qr_recorddtlinfo = pd.read_csv("contest_ext_crd_qr_recorddtlinfo.tsv",sep='\t')#机构版征信-信贷审批查询记录明细
crd_qr_recordsmr = pd.read_csv("contest_ext_crd_qr_recordsmr.tsv",sep='\t')#机构版征信-查询记录汇总
fraud = pd.read_csv("contest_fraud.tsv",sep='\t')


# In[93]:


crd_cd_ln.columns=crd_cd_ln.columns.str.upper() #列名统一大小写
crd_cd_ln_spl.columns=crd_cd_ln_spl.columns.str.upper()
crd_cd_lnd.columns=crd_cd_lnd.columns.str.upper()
crd_qr_recordsmr.columns=crd_qr_recordsmr.columns.str.upper()
crd_qr_recorddtlinfo.columns=crd_qr_recorddtlinfo.columns.str.upper()


# In[94]:


fraud['Y_FRAUD'].value_counts()


# In[95]:


basic_train['Y'].value_counts()


# In[96]:


basic_test = pd.merge(basic_test,fraud)


# In[97]:


basic_test["IS_TRAIN"]=0#mark the dataset to distinguish whether it is training set
basic_train["IS_TRAIN"]=1
basic_test.head()


# In[98]:


data = pd.merge(basic_train,basic_test,how = "outer") #merge test set with training set so that they can be handled together


# In[99]:


data['Y'].value_counts()
fraud["Y_FRAUD"].value_counts()#fraud 啥意思？？？？


# In[100]:


data = pd.merge(data,crd_hd_report,on = "REPORT_ID") #与报告主表合并
data.head()


# In[101]:


data = pd.merge(data,crd_is_creditcue,on = "REPORT_ID",how= "outer")#与信用提示表合并


# In[102]:


del crd_cd_lnd_ovd["MONTH_DW"] #删除多余数据


# In[103]:


group=crd_cd_lnd_ovd.groupby('REPORT_ID',as_index = False).sum() #分组求和 禁用索引


# In[104]:


data = pd.merge(data,group,on = "REPORT_ID",how= "outer") #与贷记卡逾期/透支记录合并


# In[105]:


data[['LAST_MONTHS','AMOUNT']]=data[['LAST_MONTHS','AMOUNT']].fillna(0) #无逾期记录的填充0


# In[106]:


data.head()


# In[107]:


del crd_qr_recordsmr["TYPE_ID"]
crd_qr_recordsmr=crd_qr_recordsmr.groupby('REPORT_ID',as_index = False).sum()


# In[108]:


crd_qr_recordsmr.head()


# In[109]:


data = pd.merge(data,crd_qr_recordsmr,on = "REPORT_ID",how= "outer") #与查询记录汇总合并
data['SUM_DW']=data['SUM_DW'].fillna(0)#无查询记录的填充0


# In[110]:


data.head()


# In[111]:


group2 = crd_is_ovdsummary.groupby('REPORT_ID',as_index = False).sum() #按REPORT_ID分组 对不同贷款种类的贷款逾期笔数、贷款逾期月份数、贷款单月最高逾期总额、最大贷款时长进行求和操作


# In[112]:


data = pd.merge(data,group2,on= "REPORT_ID",how= "outer") #与逾期(透支)信息汇总合并


# In[113]:


data[['COUNT_DW','MONTHS','HIGHEST_OA_PER_MON','MAX_DURATION']]=data[['COUNT_DW','MONTHS','HIGHEST_OA_PER_MON','MAX_DURATION']].fillna(0)#NA填补为0


# In[114]:


group3=crd_is_sharedebt.groupby('REPORT_ID',as_index = False).sum()
data = pd.merge(data,group3,on= "REPORT_ID",how= "outer")#与未销户贷记卡或者未结清贷款合并
data[['FINANCE_CORP_COUNT','FINANCE_ORG_COUNT','ACCOUNT_COUNT','CREDIT_LIMIT','MAX_CREDIT_LIMIT_PER_ORG','MIN_CREDIT_LIMIT_PER_ORG','BALANCE','USED_CREDIT_LIMIT','LATEST_6M_USED_AVG_AMOUNT'
]]=data[['FINANCE_CORP_COUNT','FINANCE_ORG_COUNT','ACCOUNT_COUNT','CREDIT_LIMIT','MAX_CREDIT_LIMIT_PER_ORG','MIN_CREDIT_LIMIT_PER_ORG','BALANCE','USED_CREDIT_LIMIT','LATEST_6M_USED_AVG_AMOUNT'
]].fillna(0)


# In[115]:


#对贷记卡数据进行处理


# In[116]:


t=pd.to_datetime(crd_cd_lnd['OPEN_DATE'])
end = pd.datetime(2017,12,31)


# In[117]:


t=end-t #开卡日期距2017年底的天数


# In[118]:


crd_cd_lnd['OPEN_DATE']=t
group4 = crd_cd_lnd[['REPORT_ID','OPEN_DATE']].groupby('REPORT_ID',as_index=False).max()
crd_cd_lnd=crd_cd_lnd.fillna(0)


# In[119]:


group5 = crd_cd_lnd[['REPORT_ID','CREDIT_LIMIT_AMOUNT','SHARE_CREDIT_LIMIT_AMOUNT','USED_CREDIT_LIMIT_AMOUNT','LATEST6_MONTH_USED_AVG_AMOUNT','USED_HIGHEST_AMOUNT','SCHEDULED_PAYMENT_AMOUNT','ACTUAL_PAYMENT_AMOUNT','CURR_OVERDUE_CYC','CURR_OVERDUE_AMOUNT'
]].groupby('REPORT_ID',as_index = False).sum()#分组求和——各种额度
crd_cd_lnd = pd.merge(group5,group4,on="REPORT_ID")#与最大开卡日期相并


# In[120]:


data = pd.merge(data,crd_cd_lnd,on="REPORT_ID",how="outer")#与贷记卡合并


# In[121]:


#处理贷款数据
group6=crd_cd_ln[["REPORT_ID","LOAN_ID"]].groupby('REPORT_ID',as_index=False).count()


# In[122]:


group7=crd_cd_ln[["REPORT_ID","CREDIT_LIMIT_AMOUNT",'BALANCE','SCHEDULED_PAYMENT_AMOUNT','ACTUAL_PAYMENT_AMOUNT','CURR_OVERDUE_CYC','CURR_OVERDUE_AMOUNT'
]].groupby('REPORT_ID',as_index=False).sum()


# In[123]:


group = pd.merge(group6,group7)
group.rename(columns={'LOAN_ID':'LOAN_NUM'},inplace=True)
data=pd.merge(data,group,on="REPORT_ID",how="outer")#与贷款数据合并


# In[124]:


group7 = crd_cd_ln_spl.groupby("REPORT_ID",as_index=False).sum()


# In[125]:


data=pd.merge(data,group7,on="REPORT_ID",how="outer")#与贷款特殊交易合并


# In[126]:


data[["CHANGING_MONTHS","CHANGING_AMOUNT"]]=data[["CHANGING_MONTHS","CHANGING_AMOUNT"]].fillna(0)


# In[127]:


#合并完成 开始进一步预处理
data.info()


# In[128]:


data["OPEN_DATE"] = data["OPEN_DATE"].dt.days#convert timedelta into float


# In[129]:


data.drop(["ID_CARD","AGENT","SALARY","Y_FRAUD","FIRST_LOAN_OPEN_MONTH","FIRST_LOANCARD_OPEN_MONTH","FIRST_SL_OPEN_MONTH"],1,inplace=True)#删除缺失过多 以及处理意义不大的列


# In[130]:


tl=pd.to_datetime(data['LOAN_DATE'])
endl = pd.datetime(2017,12,31)
data['LOAN_DATE']=endl-tl
data['LOAN_DATE']=data['LOAN_DATE'].dt.days #convert timedelta into float


# In[131]:


data[['IS_LOCAL','EDU_LEVEL','MARRY_STATUS']]=data[['IS_LOCAL','EDU_LEVEL','MARRY_STATUS']].fillna('unknown')#对属性型变量中的缺失值用unknown填充 下一步使用onehot编码


# In[132]:


t2=pd.to_datetime(data['REPORT_CREATE_TIME'])
data['REPORT_CREATE_TIME']=endl-t2
data['REPORT_CREATE_TIME']=data['REPORT_CREATE_TIME'].dt.days
data.info()


# In[133]:


p=pd.get_dummies(data[["IS_LOCAL","EDU_LEVEL","MARRY_STATUS","QUERY_REASON","QUERY_ORG"]])#对属性变量进行无序的独热编码(one-hot)


# In[134]:


data1=pd.concat([data,p],axis=1)#按列连接


# In[135]:


data1.drop(["IS_LOCAL","EDU_LEVEL","MARRY_STATUS","QUERY_REASON","QUERY_ORG"],1,inplace=True)#删去属性为object的多余变量


# In[136]:


data1.info()


# In[137]:


#缺失值处理
data1.iloc[:,[7,8,9,10,11,12,13]]=data1.iloc[:,[7,8,9,10,11,12,13]].fillna(data1.iloc[:,[7,8,9,10,11,12,13]].mean())#均值填充


# In[138]:


data1.info()


# In[142]:


data1.iloc[:,range(30,40)]
data1.iloc[:,range(30,40)]=data1.iloc[:,range(30,40)].fillna(data1.iloc[:,range(30,40)].mean())#均值填充


# In[143]:


data1.iloc[:,range(40,48)]=data1.iloc[:,range(40,48)].fillna(0)#用0值填充 buz不一定有loan


# In[153]:


data1["WORK_PROVINCE"].value_counts()
data1["WORK_PROVINCE"].fillna(350000,inplace=True)


# In[156]:


data1["HAS_FUND"].value_counts()
data1["HAS_FUND"].fillna(1,inplace=True)


# In[158]:


data1.info()


# In[188]:


train=data1[data1.IS_TRAIN==1]
train.to_csv("train.csv",index=False,encoding='utf_8_sig')


# In[187]:


test=data1[data1.IS_TRAIN==0]
test.to_csv("test.csv",index=False,encoding='utf_8_sig')


# In[184]:




