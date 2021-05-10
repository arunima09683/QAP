import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import numpy as np
import pyodbc 
import seaborn as sns 
import matplotlib.pyplot as plt
import plotly.express as px
import QAP_Functions
from statistics import mean
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import time

import SessionState  


server = 'tcp:syn-ssemkt-dwsdev01.sql.azuresynapse.net,1433' 
database = 'SQLPL_SSEMKTDW' 
username = 'smallya' 
password = 'CLXDWuser137$$mkt' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()    

#GroupedStores = pd.DataFrame([])
def app(qap_input,df1, ct_market,t2_market,t1_market,all_week,tolerance_of_missing,group_size,existflag):
    
    ####################################################################
    def html(body):
        st.markdown(body, unsafe_allow_html=True)


    def card_begin_str(header):
        return (
            "<style>div.card{background-color:#D7DCDC;border-radius: 5px;box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);transition: 0.3s;}</style>"
            '<div class="card">'
            '<div class="container">'
            f"<h3><b>{header}</b></h3>"
        )


    def card_end_str():
        return "</div></div>"


    def card(header, body):
        lines = [card_begin_str(header), f"<p>{body}</p>", card_end_str()]
        html("".join(lines))


    def br(n):
        html(n * "<br>")


    

	###################################################
    
# Decide the number of panels we have based on the number of markets
    if existflag == "No":
        panelNum = 2
        if(len([i for i in t2_market if i!= None]) != 0):
                                            panelNum = 3
                                            
        scv_by_week=qap_input.pivot(index =['MarketDes','store_division'], columns ='week_number', values ='target_stat_case_vol').reset_index()
        number_of_week = scv_by_week.shape[1] - 2
        
          
       
        qap_input['mk'] = None   
        qap_input.loc[qap_input["MarketDes"].isin(ct_market), 'mk'] = 'Control'
        qap_input.loc[qap_input["MarketDes"].isin(t1_market), 'mk'] = 'Test1'
        qap_input.loc[qap_input["MarketDes"].isin(t2_market), 'mk'] = 'Test2'
        #st.write(qap_input)
        
        #sns.violinplot(x="mk", y="target_stat_case_vol", palette="husl", data=qap_input,ax=ax1)
        fig_vio = px.violin(qap_input, y="target_stat_case_vol", box=True, # draw box plot inside the violin
                    points=False,  color="mk",# can be 'outliers', or False,
                    labels={
                         "target_stat_case_vol": "Average Stat Case Volume Per Store",
                         "mk": "Markets"
                        
                     },
                    title="Distibution of Aggregated Stat Case Volume Before Pairing"
                   )   
        fig_vio=fig_vio.update_layout(autosize=False,
    width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4))
        
        #st.write(qap_input)
        
        qap_input_ag = qap_input[["store_division","week_start_date","target_stat_case_vol","mk"]].groupby(["mk","week_start_date"]).sum().reset_index()
        qap_input_ag_temp=qap_input_ag.pivot(index ='week_start_date', columns ='mk', values ='target_stat_case_vol').reset_index()
        #fig_lin1 = px.line(qap_input_ag_temp, x='week_start_date', y=['Control', 'Test1'],labels={
                         #"target_stat_case_vol": "Total Stat Case Volume",
                         #"week_start_date": "date" 
                    # },
                    #title="Total Sales of Unpaired Data")
        
        #if panelNum==3:
            #fig_lin1 = px.line(qap_input_ag_temp, x='week_start_date', y=['Control', 'Test1','Test2'],labels={
                         #"target_stat_case_vol": "Total Stat Case Volume",
                         #"week_start_date": "date"
                    # },
                    #title="Total Sales of Unpaired Data")
        #st.plotly_chart(fig_lin1)
        
        qap_input_ag_2 = qap_input[["store_division","week_start_date","target_stat_case_vol","mk"]].groupby(["mk","week_start_date"]).mean().reset_index()
        qap_input_ag_temp_2=qap_input_ag_2.pivot(index ='week_start_date', columns ='mk', values ='target_stat_case_vol').reset_index()
        fig_lin2 = px.line(qap_input_ag_temp_2, x='week_start_date', y=['Control', 'Test1'],labels={
                         "target_stat_case_vol": "Average Stat Case Volume",
                         "week_start_date": "date" 
                     },
                    title="Average Sales of Unpaired Data")
        
        fig_lin2=fig_lin2.update_layout(autosize=False,
    width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4))
            
        if panelNum==3:
            fig_lin2 = px.line(qap_input_ag_temp_2, x='week_start_date', y=['Control', 'Test1','Test2'],labels={
                         "target_stat_case_vol": "Average Stat Case Volume",
                         "week_start_date": "date"
                     },
                    title="Average Sales of Unpaired Data",)
            fig_lin2=fig_lin2.update_layout(autosize=False,
    width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4))
            #fig_lin2=fig_lin2.update_layout(autosize=True)
        
        
        pre_avgs=qap_input[["target_stat_case_vol","mk"]].groupby(["mk"]).mean().reset_index()
        pre_avgs.columns=['group_name','Ag_SC_Vol']
        pre_avgs["period"]='Before Pairing'
        #st.write(pre_avgs)
        
        
        for i in range(scv_by_week.shape[0]):
            scv_by_week.iloc[i,2:]=QAP_Functions.integrity_check(scv_by_week.iloc[i,2:],tolerance_of_missing)
            
        sp_input=scv_by_week.dropna(axis = 0, how = 'any')
        sp_input.reset_index(inplace=True)
        sp_input.drop("index",inplace=True,axis=1)
       
        store_count_under_each_market=(len(sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"].unique()))
        
        
        if panelNum==3:
                     store_count_under_each_market=(len(sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t2_market)]["store_division"].unique()))
      
        # Calculate the difference before and after integrity check to identify about many stores are removed
        rp1 = scv_by_week.shape[0] - sp_input.shape[0] 
        
        selections_m = st.beta_expander("Selections", expanded=False)
        with selections_m:
            card("Selected Control Markets:",",".join([i for i in ct_market if i==i]))
            card("Selected Test1 Markets:",",".join([i for i in t1_market if i==i]))
            if panelNum==3:
                card("Selected Test2 Markets:",",".join([i for i in t2_market if i!=None]))
            card("Selected Group Size:",str(group_size))  
            msg0 = str(rp1)+ ' out of ' + str(scv_by_week.shape[0])+' records are removed with tolerance of missing value set to '+ str(tolerance_of_missing)
            card(msg0,"")
        
        input_data = st.beta_expander("Input Data", expanded=False)
        with input_data:
            col1, col2, col3 = st.beta_columns(3)
            @st.cache
            def load_data():
                #tab=qap_input_store.drop(['mk','marketdes'],axis=1)
                return qap_input
        
            data = load_data()

            option=col1.selectbox('Show Entries',(10,25,50,100))
            filtered_data = qap_input.head(option)
            op=st.dataframe(filtered_data)
            filtered_data=filtered_data.applymap(str)
            #st.stop()
            time.sleep(0)  
            #session_state = SessionState()

            name = col2.text_input("Search")
            
            button_sent=col3.button('Submit')
            
            if button_sent:
                #session_state.button_sent = False
                
                if 1!=1: #session_state.button_sent:
                    #st.write(session_state.name)    
                    result = name.title()
                    st.write(result)
                    filtered_data=filtered_data.applymap(str)
                    filtered_data.columns = [x.lower() for x in filtered_data.columns]
                    st.success(op.write(filtered_data.loc[ (filtered_data['store_division'].str.contains(result)) | (filtered_data['marketdes'].str.contains(result))|
                    (filtered_data['marketdes'].str.contains(result)) | (filtered_data['target_upc_count'].str.contains(result)) |
                    (filtered_data['week_number'].str.contains(result)) | (filtered_data['week_start_date'].str.contains(result))| (filtered_data['week_end_date'].str.contains(result))| (filtered_data['mk'].str.contains(result)) ])
                    )
             
        #option=st.selectbox('Show Entries',(10,25,50,100))
        #filtered_data = qap_input.head(option)            
        #st.write(qap_input)
           
            
        fig_lin2.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4) )   
        
        
        fig_vio.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4)) 
        
        before_pairing = st.beta_expander("Before Pairing", expanded=False)
        #st.set_page_config(layout="wide")
        with before_pairing:   
            
                 
            col1, col2 = st.beta_columns([2,1])

            #original = Image.open(image)
            #col1.header("Line_Chart")
            col1.plotly_chart(fig_lin2)
            
            #grayscale = original.convert('LA')
            #col2.header("Violin_Chart")
            col2.plotly_chart(fig_vio)
            #st.plotly_chart(fig_vio)
            #st.plotly_chart(fig_lin2)
        
        
        input_sub = sp_input.iloc[:,2:]
        mklist=[i for i in ct_market+t1_market+t2_market if i!=None]
        
        if panelNum==3:
            x=np.array(store_count_under_each_market)
            mkidx = np.where(x!=max(x))[0].tolist()
            mk1_stores = sp_input[sp_input["MarketDes"].isin([mklist[mkidx[0]]])]["store_division"]
            mk2_stores = sp_input[sp_input["MarketDes"].isin([mklist[mkidx[1]]])]["store_division"]
            mk3_stores = sp_input[sp_input["MarketDes"].isin([mklist[np.where(x==max(x))[0].tolist()[0]]])]["store_division"]
            
            mk1 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[mkidx[0]]])])
            mk2 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[mkidx[1]]])])
            mk3 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[np.where(x==max(x))[0].tolist()[0]]])])
        elif panelNum==2:
            mkidx=[0,1]
            mk1_stores = sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"]
            mk2_stores = sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"]
            mk1 = np.array(input_sub[sp_input["MarketDes"].isin(ct_market)])
            mk2 = np.array(input_sub[sp_input["MarketDes"].isin(t1_market)])
        dis = QAP_Functions.cross_dis(mk1,mk2,QAP_Functions.dissimilarity)
        dis.index = mk1_stores
        dis.columns= mk2_stores
        PairedStores = QAP_Functions.store_pairing(dis)
        
        if panelNum  == 3:
        
            #Define a function to calcualte the average distances 
            def avg_dif(store1, store2, store3,data=sp_input):
                #TODO: AS NUMERIC !!!!!!!!!!
                #st.write(sp_input[sp_input["store_division"]==store1].iloc[:,2:].values[0].tolist())
                vector1 = sp_input[sp_input["store_division"]==store1].iloc[:,2:].values[0].tolist()
                vector2 = sp_input[sp_input["store_division"]==store2].iloc[:,2:].values[0].tolist()
                vector3 = sp_input[sp_input["store_division"]==store3].iloc[:,2:].values[0].tolist()
                return(mean([QAP_Functions.dissimilarity(vector1,vector3),QAP_Functions.dissimilarity(vector2,vector3),QAP_Functions.dissimilarity(vector1,vector2)]))
                    
            mk3_temp = mk3
            mk3_stores_temp = mk3_stores.reset_index()
            ps = PairedStores.reset_index().drop("level_0",axis=1)
            #st.write(mk3_temp)
            #st.write(mk3_stores_temp)
            #st.write(ps)
            GroupedStores = {'store1': '1', 'store2': '2','store3':'3', 'dissimilarities': '9.23'}
            GroupedStores=pd.DataFrame(GroupedStores,index=[0])
            #GroupedStores.to_csv("GroupedStores.csv")
            #st.write(GroupedStores)
            
            while True:
                store3 = []
                avg_difference = []
                for i in range(0,ps.shape[0]):
                    vec_of_dif = list()
    
                    for k in range(0,mk3_temp.shape[0]):
                            #st.write(ps["store1"][i],ps["store2"][i],mk3_stores_temp["store_division"][k])
                            vec_of_dif.append(avg_dif(ps["store1"][i],ps["store2"][i],mk3_stores_temp["store_division"][k]))
                            
                    #st.write(vec_of_dif)
                    mn = min(vec_of_dif)
                    idx = np.where(vec_of_dif == mn)[0][0].item()
                    best_store = mk3_stores_temp.loc[idx,"store_division"]
        
                    store3.append(best_store)
                    avg_difference.append(mn)
                groupedStores = pd.DataFrame()
                groupedStores["store1"]=ps["store1"]
                groupedStores["store2"]=ps["store2"]
                groupedStores["store3"]=store3
                groupedStores["dissimilarities"]=avg_difference    
                groupedStores = groupedStores.sort_values('dissimilarities')
                
                temp_df=groupedStores[["store3","dissimilarities"]].groupby("store3").min().reset_index()
                groupedStores=temp_df.merge(groupedStores,how="left",on=["store3","dissimilarities"])
                
                GroupedStores = GroupedStores.append(groupedStores)
                
               
                
                
                ps = ps[(~ps['store1'].isin(GroupedStores["store1"]))& (~ps['store2'].isin(GroupedStores["store2"]))].reset_index().drop("level_0",axis=1)
                mk3_temp = mk3_temp[~mk3_stores_temp["store_division"].isin(GroupedStores["store3"])]
                mk3_stores_temp = mk3_stores_temp[~mk3_stores_temp["store_division"].isin(GroupedStores["store3"])].reset_index().drop("index",axis=1)
                if ps.shape[0] == 0:
                            break
                   
            
            GroupedStores=GroupedStores.reset_index()
            GroupedStores=GroupedStores.drop(0,axis=0)
            GroupedStores=GroupedStores.drop("index",axis=1)
            GroupedStores = GroupedStores.sort_values('dissimilarities')
            
            #st.write(GroupedStores)
                    
        else:
            GroupedStores = PairedStores 
        
        #slider_group_size = st.slider('Select Group Size', 1, min(store_count_under_each_market), 1)
        
        slider_group_size=min(store_count_under_each_market)
        
        GroupedStores = GroupedStores.sort_values('dissimilarities')
        GroupedStores=GroupedStores.reset_index().drop("index",axis=1)
        #GroupedStores.to_csv("python_gs.csv")
        #qap_input.to_csv("python_qap_input.csv")
        #st.write(GroupedStores)
        grouped_sub = GroupedStores.loc[0:slider_group_size]
        #st.write(grouped_sub)
        
      
        group = []
        temp3 = [0]*qap_input.shape[0]
        
        temp1 = list(np.where(qap_input["store_division"].isin(grouped_sub["store1"]),1,0))
        temp2 = list(np.where(qap_input["store_division"].isin(grouped_sub["store2"]),2,0))
        
        
        if panelNum == 3:
            temp3 = list(np.where(qap_input["store_division"].isin(grouped_sub["store3"]),3,0))
        
        
        for i in range(0,len(temp1)):
            #st.write(group[i] , temp1[i] , temp2[i] , temp3[i])
            group.append( temp1[i] + temp2[i] + temp3[i])
            
        labelled_data = qap_input
        labelled_data["group"] = group
        labelled_data = labelled_data[labelled_data["group"] != 0]
        
        #st.write(labelled_data)
        #st.write(labelled_data.shape)
        
        labelled_data["group_name"] = np.where(labelled_data["MarketDes"].isin(ct_market),'Control',np.where(labelled_data["MarketDes"].isin(t1_market),'Test1','Test2'))
        ld = labelled_data
        
        
        #sns.violinplot(x="mk", y="target_stat_case_vol", palette="husl", data=qap_input,ax=ax1)
        fig_vio2 = px.violin(ld, y="target_stat_case_vol", box=True, # draw box plot inside the violin
                    points=False,  color="group_name",# can be 'outliers', or False,
                    labels={
                         "target_stat_case_vol": "Average Stat Case Volume Per Store",
                         "group_name": "Groups"
                        
                     },
                    title="Distribution of Aggregated Stat Case Volume After Pairing"
                   )
        fig_vio2=fig_vio2.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=5 ,
        r=50,
        b=100,
        t=100,pad=4))
        
            
        ld_ag = ld[["week_start_date","group_name","target_stat_case_vol"]].groupby(["group_name","week_start_date"]).mean().reset_index()
        
        ld_ag_temp=ld_ag.pivot(index ='week_start_date', columns ='group_name', values ='target_stat_case_vol').reset_index()
        fig_lin3 = px.line(ld_ag_temp, x='week_start_date', y=['Control', 'Test1'],labels={
                         "target_stat_case_vol": "Average Stat Case Volume",
                         "week_start_date": "date" 
                     },
                    title="Average Sales of Paired Data")
        fig_lin3=fig_lin3.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4))
        
        if panelNum==3:
            fig_lin3 = px.line(ld_ag_temp, x='week_start_date', y=['Control', 'Test1','Test2'],labels={
                             "target_stat_case_vol": "Average Stat Case Volume",
                             "week_start_date": "date" 
                         },
                        title="Average Sales of Paired Data")
            fig_lin3=fig_lin3.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4))
            
        
        post_avgs = ld[["group_name","target_stat_case_vol"]].groupby(["group_name"]).mean().reset_index()
        post_avgs["period"] = 'After Pairing'
        post_avgs.rename(columns={"target_stat_case_vol":"Ag_SC_Vol"},inplace=True)
        bar_table = pre_avgs.append(post_avgs)
        
        
        
    
        fig_bar = go.Figure(data=[
            go.Bar(name='After Pairing', x=bar_table["group_name"], y=bar_table[bar_table["period"]=='After Pairing']["Ag_SC_Vol"], text=bar_table[bar_table["period"]=='After Pairing']["Ag_SC_Vol"]),
           go.Bar(name='Before Pairing', x=bar_table["group_name"], y=bar_table[bar_table["period"]=='Before Pairing']["Ag_SC_Vol"], text=bar_table[bar_table["period"]=='Before Pairing']["Ag_SC_Vol"])
        ])
        
        fig_bar.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig_bar.update_layout(barmode='group', uniformtext_minsize=8, uniformtext_mode='hide')
        fig_bar=fig_bar.update_layout(autosize=False, width=500,
    height=500,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4),title="Bar Chart")
        # Change the bar mode
        #fig_bar.update_layout()
        
        
        #fig_bar = px.bar(x=bar_table["group_name"], y=[bar_table[bar_table["period"]=='After Pairing']["Ag_SC_Vol"], bar_table[bar_table["period"]=='Before Pairing']["Ag_SC_Vol"]],barmode='group')

        #texts = [bar_table[bar_table["period"]=='After Pairing']["Ag_SC_Vol"], bar_table[bar_table["period"]=='Before Pairing']["Ag_SC_Vol"]]
        #for i, t in enumerate(texts):
            #fig_bar.data[i].text = t
            #fig_bar.data[i].textposition = 'outside'
        
        
        #post_avgs_temp=bar_table.pivot(index ='group_name', columns ='period', values ='Ag_SC_Vol').reset_index()
       # st.write(post_avgs)
        #st.write(post_avgs_temp)
        #fig = px.bar(post_avgs_temp, x="group_name", y=["After Pairing","Before Pairing"], barmode='group', height=400)
    
        #st.plotly_chart(fig_bar)
        
        
       
        
       
        after_pairing = st.beta_expander("After Pairing", expanded=False)
        
        with after_pairing:  
            
  
            col3,col4 = st.beta_columns([2,1])
            col3.plotly_chart(fig_vio2, use_container_width = False)
            col4.plotly_chart(fig_bar)
            
        Differences = []
        
        for i in range(0,GroupedStores.shape[0]):
            
            sub_GroupedStores = GroupedStores.loc[0:i]
            
            temp_tb = qap_input[qap_input["store_division"].isin(list(sub_GroupedStores["store1"])+list(sub_GroupedStores["store2"]))]
            if panelNum == 3:
                 temp_tb = qap_input[qap_input["store_division"].isin(list(sub_GroupedStores["store1"])+list(sub_GroupedStores["store2"])+list(sub_GroupedStores["store3"]))]
            
            #st.write(temp_tb)
            
            temp_group = []
            temp3 = [0]*temp_tb.shape[0]
            
            temp1 = list(np.where(temp_tb["store_division"].isin(sub_GroupedStores["store1"]),1,0))
            temp2 = list(np.where(temp_tb["store_division"].isin(sub_GroupedStores["store2"]),2,0))
        
        
            if panelNum == 3:
                temp3 = list(np.where(temp_tb["store_division"].isin(sub_GroupedStores["store3"]),3,0))
        
        
            for i in range(0,len(temp1)):
                #st.write(group[i] , temp1[i] , temp2[i] , temp3[i])
                temp_group.append( temp1[i] + temp2[i] + temp3[i])
            
        
            temp_tb["group"] = temp_group
            temp_tb = temp_tb[temp_tb["group"] != 0]
        
              
            temp_tb_ag=temp_tb[["store_division","week_number","group","target_stat_case_vol"]].groupby(["week_number","group"]).mean().reset_index().sort_values(["group","week_number"])
            
            record1 = list(temp_tb_ag[temp_tb_ag["group"] == 1]["target_stat_case_vol"])
            record2 = list(temp_tb_ag[temp_tb_ag["group"] == 2]["target_stat_case_vol"])
            record3 = list(temp_tb_ag[temp_tb_ag["group"] == 3]["target_stat_case_vol"])
            
            D = QAP_Functions.dissimilarity(record1,record2)
            
            if(panelNum == 3):
              D = (QAP_Functions.dissimilarity(record1,record2) + QAP_Functions.dissimilarity(record1,record3) + QAP_Functions.dissimilarity(record2,record3))/3
            
            
            Differences.append(D)
        #st.write(Differences)
        GroupedStores["Differences"] = Differences
        
#=====================Trade Off==============================
#=====================Trade Off==============================
       
    
        #st.write(qap_input)
        '''cov_names=qap_input.drop(['group_name',"period","target_stat_case_vol","MarketC","MarketDes","store_division",
                "week_number","week_start_date","mk"], axis=1)
        tmp_gp = qap_input[['group_name','store_division']].groupby(['group_name']).agg().reset_index()
        st.write(tmp_gp)'''
        temp_GroupedStores = GroupedStores.copy()
        
        #st.write(temp_GroupedStores)
        trade_off = st.beta_expander("Trade Off plot", expanded=False)
        with trade_off:
            slider_group_size = st.slider('Select Group Size', 1, min(store_count_under_each_market), 1)
            col5,col6 = st.beta_columns([2,1])            
            fig_trade, ax_trade = plt.subplots(figsize=(12, 6))
            ax_trade.plot([i+1 for i in range(0,temp_GroupedStores.shape[0])],temp_GroupedStores["Differences"],  marker='o', color='black')
            fig_trade.suptitle('Trade off plot', fontsize=12)
            ax_trade.set_xlabel('Number of Triplets included', fontsize=10)
            ax_trade.set_ylabel('Average dissimilarity among groups', fontsize='medium')
            ax_trade.vlines(min(temp_GroupedStores["Differences"]),min(temp_GroupedStores["Differences"]), max(temp_GroupedStores["Differences"]) , linestyles='dashed', colors='red',label="Suggested")
            ax_trade.vlines(slider_group_size ,min(temp_GroupedStores["Differences"]), max(temp_GroupedStores["Differences"]), linestyles='dashed', colors='blue', label="Selected")
            ax_trade.legend()
            #ax_trade.vline.([Differences.index(min( Differences ))+1,slider_group_size], min(Differences), max(Differences), linestyles='dashed', colors='blue')
            #card("Suggested Group Size:",str(Differences.index(min( Differences ))))
            #st.pyplot()
            
            col5.pyplot()
            col6.plotly_chart(fig_lin3)
            
        
        data_upload = st.beta_expander("Upload Data", expanded=False)
        with data_upload:
            temp = []  
            def uploading(df):
                #df = pd.read_csv("df1.csv")
                df3 = df.head(10)
                for index, row in df3.iterrows():
                    temp.append((row.all_retailers_str, row.mk, row.group_number, row.group_name, row.store_group_name, row.author, row.all_store_str,row.all_category_str, row.all_prodtype_str, row.all_div_nbr_str, row.target_brands_str, row.comp_brands_str, row.target_subbrands_str, row.comp_subbrands_str, row.tolerance_of_missing, row.all_exclude_upc, row.marketc, row.marketdes, row.store_division, row.week_number, row.week_start_date, row.target_upc_count, row.target_stat_case_vol, row.target_avg_price, row.comp_upc_count, row.comp_stat_case_vol,row.comp_avg_price))
                
                q= """SET QUOTED_IDENTIFIER ON insert into qap.storegroups(all_retailers_str, mk, group_number, group_name, store_group_name,author, all_store_str,all_category_str, all_prodtype_str, all_div_nbr_str, target_brands_str, comp_brands_str, target_subbrands_str, comp_subbrands_str, tolerance_of_missing, all_exclude_upc, marketc, marketdes, store_division, week_number, week_start_date, target_upc_count, target_stat_case_vol, target_avg_price, comp_upc_count, comp_stat_case_vol,comp_avg_price ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                cursor.executemany(q, temp)
                cnxn.commit()
                return "Successfully Uploaded"
            
            @st.cache(suppress_st_warning=True)
            def buton():   
                result = "abc"
                return result
            if(st.button("Upload to Azure SQLDW")):
                result = buton()
                st.write('result: %s' % result)
       
            
            
        
        
        
############### Flag Yes  ##########################  
    elif existflag == "Yes":
            #st.write(groupedStores)
        ct_market = qap_input[qap_input["group_name"] == "control"]["marketdes"].unique()
        t1_market = qap_input[qap_input["group_name"] == "test1"]["marketdes"].unique()
        t2_market = qap_input[qap_input["group_name"] == "test2"]["marketdes"].unique()
        tolerance_of_missing = float(qap_input["tolerance_of_missing"][0])
        
            
        starting_week = min(qap_input["week_number"])
        ending_week = max(qap_input["week_number"])
        author = qap_input["author"][0]
        #panelNum = if len(t2_market).isin ([0, 2,3])
        panelNum = 2
        if(len([ t2_market ]) != 0):
                                            panelNum = 3
                                            
        #panelNum=[2 if x==0 else 3 for x in len(t2_market)]
            
        qap_input_store = qap_input[["marketdes","store_division","week_start_date","week_number","target_upc_count","target_stat_case_vol",
                                                                            "target_avg_price","comp_upc_count",
                                                                            "comp_stat_case_vol","comp_avg_price",
                                                                           "group_name","mk"]]
            
            # Extract the user selections in generating this store group
        stored_retailers = qap_input["all_retailers_str"][0]
        stored_store = qap_input["all_store_str"][0]
        stored_category = qap_input["all_category_str"][0]
        stored_prodtype = qap_input["all_prodtype_str"][0]
        
         
        stored_target_brands = qap_input["target_brands_str"][0]
        stored_comp_brands = qap_input["comp_brands_str"][0]
        stored_target_subbrands = qap_input["target_subbrands_str"][0]
        stored_comp_subbrands = qap_input["comp_subbrands_str"][0]
        date_created = qap_input["date_created"][0]
            
        stored_group_size = len(qap_input[qap_input["group_name"] == "control"]["store_division"].unique())
        
        Selection = st.beta_expander("Selection", expanded=False)                
        with Selection:
            st.write("Date Created:",date_created)
            st.write("Retailers:",stored_retailers)
            st.write("Store:",stored_store)
            st.write("Category:",stored_category)
            st.write("Product Type:",stored_prodtype)
            st.write("Target Brands:",stored_target_brands)
            st.write("Target Sub-Brands:",stored_target_subbrands)
            st.write("Competitor Brands:",stored_comp_brands)
            st.write("Competitor Sub-Brands:",stored_comp_subbrands)
            st.write("Control Market:", ",".join (list(ct_market)))
            st.write("Test1 Market:",",".join (list(t1_market)))
            st.write("Test2 Market:",",".join (list(t2_market)))
            st.write("Pre Test Starting:",starting_week)
            st.write("Pre Test Ending:",ending_week)
            st.write("Group Size:",stored_group_size)
            st.write("Tolerance of Mising:",tolerance_of_missing)
        
        
        #withProgress(message = "Plotting",value = 0.6,{
        
        ## Construct the Box/violin plots ,all_week,tolerance_of_missing,group_size
        tab=qap_input_store.drop(['mk','marketdes'],axis=1)
        
        input_data = st.beta_expander("Input Data", expanded=False)
        with input_data:
           
            col1, col2, col3 = st.beta_columns(3)
            @st.cache
            def load_data():
                #tab=qap_input_store.drop(['mk','marketdes'],axis=1)
                return tab
        
            data = tab

            option=col1.selectbox('Show Entries',(10,25,50,100))
            filtered_data = tab.head(option)
            op=st.dataframe(filtered_data)
            filtered_data=filtered_data.applymap(str)
            #st.stop()
            time.sleep(0)  
            #session_state = SessionState()

            name = col2.text_input("Search")
            
            button_sent=col3.button('Submit')
            
            if button_sent:
                #session_state.button_sent = False
                
                if 1!=1: #session_state.button_sent:
                    #st.write(session_state.name)    
                    result = name.title()
                    st.write(result)
                    filtered_data=filtered_data.applymap(str)
                    filtered_data.columns = [x.lower() for x in filtered_data.columns]
                    st.success(op.write(filtered_data.loc[ (filtered_data['store_division'].str.contains(result)) | (filtered_data['marketdes'].str.contains(result))|
                    (filtered_data['marketdes'].str.contains(result)) | (filtered_data['target_upc_count'].str.contains(result)) |
                    (filtered_data['week_number'].str.contains(result)) | (filtered_data['week_start_date'].str.contains(result))| (filtered_data['week_end_date'].str.contains(result))| (filtered_data['mk'].str.contains(result)) ])
                    )
                    
        Average_Plot = st.beta_expander("Average_Plot", expanded=False)                
        with Average_Plot:
                          
            qap_input_ag = qap_input_store[['store_division','week_start_date','group_name','target_stat_case_vol']]. groupby(['group_name','week_start_date']).mean('target_stat_case_vol').reset_index()
            qap_input_ag_temp=qap_input_ag.pivot(index ='week_start_date', columns ='group_name', values ='target_stat_case_vol').reset_index()
        
        
            fig_lin = px.line(qap_input_ag_temp, x='week_start_date', y=['control','test1','test2'],labels={
                             "target_stat_case_vol": "Average Stat Case Volume",
                             "week_start_date": "date" 
                         }, title="Average Sales of Test and Control Group")
                    
        
            fig_lin=fig_lin.update_layout(autosize=False)
        
            st.plotly_chart(fig_lin)
              
        fig_vio = px.violin(qap_input_store, y="target_stat_case_vol", box=True, # draw box plot inside the violin
                    points=False,  color="group_name",# can be 'outliers', or False,
                    labels={
                         "target_stat_case_vol": "Average Stat Case Volume Per Store",
                         "group_name": "Groups"
                        
                     },
                    title="Distibution of Aggregated Stat Case Volume"
                   )
        #st.plotly_chart(fig_vio)
        
        
     
      
        
            ## Make a table for the bar plot
        #print(qap_input_store.columns)
        temp=qap_input_store[['mk','target_stat_case_vol']].groupby('mk').mean('target_stat_case_vol').reset_index()
        temp=temp.rename(columns={'target_stat_case_vol':'Average Stat Case Volume Per Store Per week'})
        temp=temp.rename(columns={'mk':'group name'})
        
        #df = px.data.gapminder().query("continent == 'Europe' and year == 2007 and pop > 2.e6") 
        fig = px.bar(temp, y='Average Stat Case Volume Per Store Per week', x='group name',text='Average Stat Case Volume Per Store Per week') 
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside') 
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide') 
        #st.plotly_chart(fig)
        
        
        Chart = st.beta_expander("Charts", expanded=False)                
        with Chart:
            col5,col6 = st.beta_columns([2,1])
            col5.plotly_chart(fig_vio, use_container_width=False)
            col6.plotly_chart(fig, use_container_width=False)
        
        #st.write(bar_table)
        
        #st.write(GroupedStores)
    return        


def app1(qap_input,ct_market,t2_market,t1_market,all_week,tolerance_of_missing,group_size,existflag):
    
    ####################################################################
    def html(body):
        st.markdown(body, unsafe_allow_html=True)


    def card_begin_str(header):
        return (
            "<style>div.card{background-color:#D7DCDC;border-radius: 5px;box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);transition: 0.3s;}</style>"
            '<div class="card">'
            '<div class="container">'
            f"<h3><b>{header}</b></h3>"
        )


    def card_end_str():
        return "</div></div>"


    def card(header, body):
        lines = [card_begin_str(header), f"<p>{body}</p>", card_end_str()]
        html("".join(lines))


    def br(n):
        html(n * "<br>")


    

	###################################################
    
# Decide the number of panels we have based on the number of markets
    if 1 == 1:
        
        panelNum = 2
        if(len([i for i in t2_market if i!= None]) != 0):
                                            panelNum = 3
                                            
        scv_by_week=qap_input.pivot(index =['MarketDes','store_division'], columns ='week_number', values ='target_stat_case_vol').reset_index()
        number_of_week = scv_by_week.shape[1] - 2
        
          
       
        qap_input['mk'] = None   
        qap_input.loc[qap_input["MarketDes"].isin(ct_market), 'mk'] = 'Control'
        qap_input.loc[qap_input["MarketDes"].isin(t1_market), 'mk'] = 'Test1'
        qap_input.loc[qap_input["MarketDes"].isin(t2_market), 'mk'] = 'Test2'
        
        qap_input_ag = qap_input[["store_division","week_start_date","target_stat_case_vol","mk"]].groupby(["mk","week_start_date"]).sum().reset_index()
        qap_input_ag_temp=qap_input_ag.pivot(index ='week_start_date', columns ='mk', values ='target_stat_case_vol').reset_index()

        
        qap_input_ag_2 = qap_input[["store_division","week_start_date","target_stat_case_vol","mk"]].groupby(["mk","week_start_date"]).mean().reset_index()
        qap_input_ag_temp_2=qap_input_ag_2.pivot(index ='week_start_date', columns ='mk', values ='target_stat_case_vol').reset_index()
        
        
        pre_avgs=qap_input[["target_stat_case_vol","mk"]].groupby(["mk"]).mean().reset_index()
        pre_avgs.columns=['group_name','Ag_SC_Vol']
        pre_avgs["period"]='Before Pairing'
        #st.write(pre_avgs)
        
        
        for i in range(scv_by_week.shape[0]):
            scv_by_week.iloc[i,2:]=QAP_Functions.integrity_check(scv_by_week.iloc[i,2:],tolerance_of_missing)
            
        sp_input=scv_by_week.dropna(axis = 0, how = 'any')
        sp_input.reset_index(inplace=True)
        sp_input.drop("index",inplace=True,axis=1)
       
        store_count_under_each_market=(len(sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"].unique()))
        if panelNum==3:
                     store_count_under_each_market=(len(sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"].unique()),
                                       len(sp_input[sp_input["MarketDes"].isin(t2_market)]["store_division"].unique()))
      
        # Calculate the difference before and after integrity check to identify about many stores are removed
        rp1 = scv_by_week.shape[0] - sp_input.shape[0] 
               
        
        input_sub = sp_input.iloc[:,2:]
        mklist=[i for i in ct_market+t1_market+t2_market if i!=None]
        
        if panelNum==3:
            x=np.array(store_count_under_each_market)
            mkidx = np.where(x!=max(x))[0].tolist()
            mk1_stores = sp_input[sp_input["MarketDes"].isin([mklist[mkidx[0]]])]["store_division"]
            mk2_stores = sp_input[sp_input["MarketDes"].isin([mklist[mkidx[1]]])]["store_division"]
            mk3_stores = sp_input[sp_input["MarketDes"].isin([mklist[np.where(x==max(x))[0].tolist()[0]]])]["store_division"]
            
            mk1 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[mkidx[0]]])])
            mk2 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[mkidx[1]]])])
            mk3 = np.array(input_sub[sp_input["MarketDes"].isin([mklist[np.where(x==max(x))[0].tolist()[0]]])])
        elif panelNum==2:
            mkidx=[0,1]
            mk1_stores = sp_input[sp_input["MarketDes"].isin(ct_market)]["store_division"]
            mk2_stores = sp_input[sp_input["MarketDes"].isin(t1_market)]["store_division"]
            mk1 = np.array(input_sub[sp_input["MarketDes"].isin(ct_market)])
            mk2 = np.array(input_sub[sp_input["MarketDes"].isin(t1_market)])
        dis = QAP_Functions.cross_dis(mk1,mk2,QAP_Functions.dissimilarity)
        dis.index = mk1_stores
        dis.columns= mk2_stores
        PairedStores = QAP_Functions.store_pairing(dis)
        if panelNum  == 3:
        
            #Define a function to calcualte the average distances 
            def avg_dif(store1, store2, store3,data=sp_input):
                #TODO: AS NUMERIC !!!!!!!!!!
                #st.write(sp_input[sp_input["store_division"]==store1].iloc[:,2:].values[0].tolist())
                vector1 = sp_input[sp_input["store_division"]==store1].iloc[:,2:].values[0].tolist()
                vector2 = sp_input[sp_input["store_division"]==store2].iloc[:,2:].values[0].tolist()
                vector3 = sp_input[sp_input["store_division"]==store3].iloc[:,2:].values[0].tolist()
                return(mean([QAP_Functions.dissimilarity(vector1,vector3),QAP_Functions.dissimilarity(vector2,vector3),QAP_Functions.dissimilarity(vector1,vector2)]))
                    
            mk3_temp = mk3
            mk3_stores_temp = mk3_stores.reset_index()
            ps = PairedStores.reset_index().drop("level_0",axis=1)
            #st.write(mk3_temp)
            #st.write(mk3_stores_temp)
            #st.write(ps)
            GroupedStores = {'store1': '1', 'store2': '2','store3':'3', 'dissimilarities': '9.23'}
            GroupedStores=pd.DataFrame(GroupedStores,index=[0])
            #GroupedStores.to_csv("GroupedStores.csv")
            
            
            while True:
                store3 = []
                avg_difference = []
                for i in range(0,ps.shape[0]):
                    vec_of_dif = list()
    
                    for k in range(0,mk3_temp.shape[0]):
                            #st.write(ps["store1"][i],ps["store2"][i],mk3_stores_temp["store_division"][k])
                            vec_of_dif.append(avg_dif(ps["store1"][i],ps["store2"][i],mk3_stores_temp["store_division"][k]))
                            
                    #st.write(vec_of_dif)
                    mn = min(vec_of_dif)
                    idx = np.where(vec_of_dif == mn)[0][0].item()
                    best_store = mk3_stores_temp.loc[idx,"store_division"]
        
                    store3.append(best_store)
                    avg_difference.append(mn)
                groupedStores = pd.DataFrame()
                groupedStores["store1"]=ps["store1"]
                groupedStores["store2"]=ps["store2"]
                groupedStores["store3"]=store3
                groupedStores["dissimilarities"]=avg_difference    
                groupedStores = groupedStores.sort_values('dissimilarities')
                
                temp_df=groupedStores[["store3","dissimilarities"]].groupby("store3").min().reset_index()
                groupedStores=temp_df.merge(groupedStores,how="left",on=["store3","dissimilarities"])
                
                GroupedStores = GroupedStores.append(groupedStores)
                
               
                
                
                ps = ps[(~ps['store1'].isin(GroupedStores["store1"]))& (~ps['store2'].isin(GroupedStores["store2"]))].reset_index().drop("level_0",axis=1)
                mk3_temp = mk3_temp[~mk3_stores_temp["store_division"].isin(GroupedStores["store3"])]
                mk3_stores_temp = mk3_stores_temp[~mk3_stores_temp["store_division"].isin(GroupedStores["store3"])].reset_index().drop("index",axis=1)
                if ps.shape[0] == 0:
                            break
                   
            
            GroupedStores=GroupedStores.reset_index()
            GroupedStores=GroupedStores.drop(0,axis=0)
            GroupedStores=GroupedStores.drop("index",axis=1)
            GroupedStores = GroupedStores.sort_values('dissimilarities')
            #st.write(GroupedStores)
                    
        else:
            GroupedStores = PairedStores 
        
        #slider_group_size = st.slider('Select Group Size', 1, min(store_count_under_each_market), 1)
        
        slider_group_size=min(store_count_under_each_market)
        
        GroupedStores = GroupedStores.sort_values('dissimilarities')
        GroupedStores=GroupedStores.reset_index().drop("index",axis=1)
        
    return  GroupedStores    
              
