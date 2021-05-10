import Store_Groups
#import In_Market_Tracking
import ANCOVA_with_RandomSample
import ANCOVA
import streamlit as st
import pandas as pd
import numpy as np
import pyodbc 
import seaborn as sns 
import matplotlib.pyplot as plt
import plotly.express as px
import QAP_Functions
from datetime import timedelta
from statistics import mean
from datetime import date
import sys
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from streamlit import cli as stcli

#import SessionState


#st.write("hello")
#@st.cache(suppress_st_warning=True)
def main():
    st.set_option('deprecation.showPyplotGlobalUse', False)
    #st.set_page_config(layout="wide")
    st.markdown(
        """<style>
            .body {text-align: left !important}
        </style>
        """, unsafe_allow_html=True)
    #st.markdown('<style>body{background-color: Blue;}</style>',unsafe_allow_html=True)
    
    PAGES = {
        "Store Groups": Store_Groups,
    	"In Market Tracking": '',
        "ANCOVA": ANCOVA,
    	"ANCOVA with Random Sample": ANCOVA_with_RandomSample,
    }
    
    st.sidebar.title('Navigation')
    st.markdown('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
    #st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
    #st.title("QAP Dashboard")
    
    st.markdown("<h1 style='text-align: center; color: black;'>QAP Dashboard</h1>", unsafe_allow_html=True)
    selection = st.radio("Go to", list(PAGES.keys()))
    page = PAGES[selection]
    #page = ''
    #c1, c2, c3, c4 = st.beta_columns(4)
    #if c1.button('Store Pairing'):
       # page = PAGES["Store Groups"]
    #elif c2.button("In Market Tracking"):
        #page = PAGES["In Market Tracking"]
    #elif c3.button("ANCOVA"):
        #page = PAGES["ANCOVA"]
    #elif c4.button("ANCOVA with Random Sample"):
         #page = PAGES["ANCOVA with Random Sample"]
         
         
         
         
            
    
    
    ####################### SERVER ###############
   
    server = 'tcp:syn-ssemkt-dwsdev01.sql.azuresynapse.net,1433' 
    database = 'SQLPL_SSEMKTDW' 
    username = 'smallya' 
    password = 'CLXDWuser137$$mkt' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    #query='SELECT COUNT (*) FROM SQLPL_SSEMKTDW.qap.qta_dash_filter_agg'
    #df = pd. read_sql(query, cnxn)
    #st.write(df)
    
    
    table_name="qap.clx_comp_weekly_sales"
    store_groups_table_name= "qap.storegroups"
    dev_table_name = table_name
    filter_table_name="qap.qta_dash_filter_agg"
    
    def card_begin_str(header):
            return (
                "<style>div.card{background-color:#D7DCDC;border-radius: 5px;box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);transition: 0.3s;}</style>"
                '<div class="card">'
                '<div class="container">'
                f"<h3><b>{header}</b></h3>"
            )
    
    
    def card_end_str():
        return "</div></div>"
        
    
    def html(body):
        st.markdown(body, unsafe_allow_html=True) 
        
                   
        
    def card(header, body):
        lines = [card_begin_str(header), f"<p>{body}</p>", card_end_str()]
        html("".join(lines))
            
    
    #########################
    
    #Loading data
    @st.cache(suppress_st_warning=True)
    def load_data():
    
        #ODBC Server connection
    
        server = 'tcp:syn-ssemkt-dwsdev01.sql.azuresynapse.net,1433' 
        database = 'SQLPL_SSEMKTDW' 
        username = 'smallya' 
        password = 'CLXDWuser137$$mkt' 
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        #query='SELECT COUNT (*) FROM SQLPL_SSEMKTDW.qap.qta_dash_filter_agg'
        #df = pd. read_sql(query, cnxn)
        #st.write(df)
    
    
        table_name="qap.clx_comp_weekly_sales"
        store_groups_table_name= "qap.storegroups"
        dev_table_name = table_name
        filter_table_name="qap.qta_dash_filter_agg"
        
        
        query = "".join(["SELECT DISTINCT store_group_name, store_division, marketdes FROM ", store_groups_table_name])
        all_store_group_names = pd.read_sql(query, cnxn)
        #st.write(all_store_group_names)
    
        query="SELECT DISTINCT retailer_name, store_banner,iri_sub_category AS category, div_nbr, prod_type, brand, subbrand FROM "+ filter_table_name
        all_hierarchy=pd.read_sql(query, cnxn)
        #st.write(all_hierarchy)
    
    
        query="SELECT DISTINCT week_start_date, week_number, week_end_date,retailer_name, store_division, nielsen_desig_market_area_desc FROM "+ dev_table_name+" ORDER BY week_number"
        all_week=pd.read_sql(query, cnxn)
        #st.write(all_week)
        #st.write(all_week.dtypes)
        #all_week["week_start_date"]=pd.datetime(all_week["week_start_date"])
    
        temp_table="qap.clx_comp_markets"
        query="SELECT DISTINCT retailer_name,market_name,RIGHT(mkt_state,(CHARINDEX(' ',REVERSE(mkt_state),0))) as mkt_state FROM "+temp_table+" ORDER BY mkt_state, market_name, retailer_name"
        all_markets_temp= pd.read_sql(query, cnxn)
        all_markets=list(all_markets_temp["market_name"])
        #st.write(all_markets_temp)
        return all_store_group_names,all_hierarchy,all_week,all_markets,all_markets_temp
        
    all_store_group_names,all_hierarchy,all_week,all_markets,all_markets_temp =load_data()
        
    ################
    
    #================== Main Sidebar Start=====================
    
    #filtered_market = st.sidebar.selectbox("Select Market", table_name["market_name"].unique())
    default_ix_yn=["Yes","No"].index("No")
    existFlag= st.sidebar.selectbox("Use Existing Store Pairs?", ["Yes","No"],index=default_ix_yn)
    
    if existFlag=="No":
        
        
        #Retailer
       # default_ix_retail = list(all_hierarchy["retailer_name"].unique()).index("Kroger")
        #retailer=st.sidebar.selectbox("Please enter the Retailer", list(all_hierarchy["retailer_name"].unique()),index=default_ix_retail)
        
        retailer_list=list(all_hierarchy["retailer_name"].unique())
        retailer_list_select_all=["Select all"]+retailer_list
        
    
        retailerChoiceInfo=st.sidebar.multiselect("Please enter the Retailer", retailer_list_select_all,default=["Kroger"])
        if "Select all" in retailerChoiceInfo:
            retailerChoiceInfo= retailer_list_select_all[1:]
        elif "Kroger" in retailerChoiceInfo and "Target"  in retailerChoiceInfo:
            retailerChoiceInfo= retailer_list_select_all[1:]
            
        retailer=retailerChoiceInfo
        retailer=[i for i in retailer if i!= None]
        retailer1=retailer[0]
        
        #Store banner
        if len(retailer)>1:
            
            store_banner_list1=list(all_hierarchy[all_hierarchy["retailer_name"].isin([retailer[0]])]["store_banner"].dropna().unique())
            store_banner_list2 = list(all_hierarchy[all_hierarchy["retailer_name"].isin([retailer[1]])]["store_banner"].dropna().unique())
            store_banner_list = store_banner_list1 + store_banner_list2
            store_banner_list = list(set(store_banner_list))
            if len(store_banner_list) == 0:
                st.write("There is no data present.")
                
            
            
        else:
           
            store_banner_list=list(all_hierarchy[all_hierarchy["retailer_name"].isin([retailer[0]])]["store_banner"].dropna().unique())
            if len(store_banner_list) == 0:
                st.write("There is no data present.")
        store_banner_select_all=["Select all"]+store_banner_list
    
        storeBannerChoiceInfo=st.sidebar.multiselect("Please select store banners", store_banner_select_all,default=["Select all"],  key = '1')
        if "Select all" in storeBannerChoiceInfo:
            storeBannerChoiceInfo=store_banner_select_all[1:]
        #st.write(storeBannerChoiceInfo)    
        store=storeBannerChoiceInfo
        store=[i for i in store if i!= None]
        
        
        #Control market
        #default_ix_ct = all_markets.index("Indianapolis IN")
        #ct_market= st.sidebar.selectbox("Please enter the control markets", all_markets,index=default_ix_ct)
        if len(retailer)>1:
            control_banner_list1=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
            control_banner_list2=list(all_week[all_week["retailer_name"].isin([retailer[1]])]["nielsen_desig_market_area_desc"].unique())
            control_banner_list = control_banner_list1 + control_banner_list2
            control_banner_list = list(set(control_banner_list))
            if len(control_banner_list) == 0:
                st.write("There is no data present.")
        else:
             control_banner_list=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
             if len(control_banner_list) == 0:
                st.write("There is no data present.")
                
        control_banner_select_all=["Select all"]+control_banner_list
        #st.write(control_banner_select_all)
        #remove nan
        #store_banner_select_all=[i for i in store_banner_select_all if i==i]
        ct_market= st.sidebar.multiselect("Please enter the control markets", control_banner_select_all,default="Indianapolis IN")
        if len(ct_market) == 0:
            st.write("Please enter the test1 markets")
        else:    
            ct_market1 = ct_market[0]
       
        
         #Store division
        flatList = [] 
        if len(ct_market)>1:
            for i in range(len(ct_market)):
                for j in range(len(retailer)):
                    store_division_lis1=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([ct_market[i]])) &
                                              (all_week["retailer_name"].isin([retailer[j]]))]["store_division"].unique())
                    
                    flatList.append(store_division_lis1)
                    store_division_list = [ item for elem in flatList for item in elem]
                    if len(store_division_list) == 0:
                        st.write("There is no data present.")
                    
        else:    
            store_division_list=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([ct_market1])) &
                                          (all_week["retailer_name"].isin([retailer[0]]))]["store_division"].unique())
            if len(store_division_list) == 0:
                st.write("There is no data present.")
        #st.write(store_division_list)     
        store_division_select_all=["Select all"]+store_division_list
        #remove nan
        #store_banner_select_all=[i for i in store_banner_select_all if i==i]
        storeDivisionChoiceInfo=st.sidebar.multiselect("Please select store numbers", store_division_select_all,default=["Select all"], key = '1')
        if "Select all" in storeDivisionChoiceInfo:
            storeDivisionChoiceInfo=store_division_select_all[1:]
        store1=storeDivisionChoiceInfo
        store1=[i for i in store if i!= None]
        
        #Test 1 market
        if len(retailer)>1:
            test1_banner_list1=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
            test1_banner_list2=list(all_week[all_week["retailer_name"].isin([retailer[1]])]["nielsen_desig_market_area_desc"].unique())
            test1_banner_list = test1_banner_list1 + test1_banner_list2
            test1_banner_list = list(set(test1_banner_list))
            if len(test1_banner_list) == 0:
                st.write("There is no data present.")
            
        else:
             test1_banner_list=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
             if len(test1_banner_list) == 0:
                st.write("There is no data present.")
        test1_banner_select_all=["Select all"]+test1_banner_list
        t1_market= st.sidebar.multiselect("Please enter the test1 markets", test1_banner_select_all ,default="Nashville TN", key = '8')
        if len(t1_market) == 0:
            st.write("Please enter the test1 markets")
        else:    
            t1_market1 = t1_market[0]
        
        #st.write(t1_market.dtypes)
         #Store division
        #store_division_list1=list(all_store_group_names[all_store_group_names["marketdes"].isin([t1_market])]["store_division"].unique())
        flatList1 = [] 
        if len(t1_market)>1:
            for i in range(len(t1_market)):
                for j in range(len(retailer)):
                    store_division_lis2=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([t1_market[i]]))  &
                                          (all_week["retailer_name"].isin([retailer[j]])) &
                                          (~all_week["store_division"].isin([store_division_list]))]["store_division"].unique())
                    flatList1.append(store_division_lis2)
                    store_division_list1 = [ item for elem in flatList1 for item in elem]
                    
                    
        else:           
            store_division_list1=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([t1_market[0]]))  &
                                          (all_week["retailer_name"].isin([retailer[0]])) &
                                          (~all_week["store_division"].isin([store_division_list]))]["store_division"].unique())
        
        store_division_select_all1=["Select all"]+store_division_list1
        #remove nan
        #store_banner_select_all=[i for i in store_banner_select_all if i==i]
        storeDivisionChoiceInfo1=st.sidebar.multiselect("Please select store numbers", store_division_select_all1,default=["Select all"], key = '2')
        if "Select all" in storeDivisionChoiceInfo1:
            storeDivisionChoiceInfo1=store_division_select_all1[1:]
        store2=storeDivisionChoiceInfo1
        store2=[i for i in store if i!= None]
        
        
        
        
        #Test 2 market
        #Test 1 market
        if len(retailer)>1:
            test2_banner_list1=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
            test2_banner_list2=list(all_week[all_week["retailer_name"].isin([retailer[1]])]["nielsen_desig_market_area_desc"].unique())
            test2_banner_list = test2_banner_list1 + test2_banner_list2
            test2_banner_list = list(set(test2_banner_list))
            if len(test2_banner_list) == 0:
                st.write("There is no data present.")
        else:    
            test2_banner_list=list(all_week[all_week["retailer_name"].isin([retailer[0]])]["nielsen_desig_market_area_desc"].unique())
            if len(test2_banner_list) == 0:
                st.write("There is no data present.")
        test2_banner_select_all=["Select all"]+test2_banner_list
        t2_market= st.sidebar.multiselect("Please enter the test2 markets", test2_banner_select_all, key = '9')
        if len(t2_market) == 0:
            st.write("Please enter the test1 markets")
        else:    
            t2_market1 = t2_market[0]
        
        
         #Store division
        #store_division_list2=list(all_store_group_names[all_store_group_names["marketdes"].isin([t2_market])]["store_division"].unique())
        flatList2 = [] 
        if len(t2_market)>1:
            for i in range(len(t2_market)):
                for j in range(len(retailer)):
                    store_division_lis2=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([t2_market[i]])) &
                                              (all_week["retailer_name"].isin([retailer[j]])) &
                                              (~all_week["store_division"].isin([store_division_list])) &
                                              (~all_week["store_division"].isin([store_division_list1]))]["store_division"].unique())
                    flatList2.append(store_division_lis2)
                    store_division_list2 = [ item for elem in flatList2 for item in elem]
                    
        else:            
            store_division_list2=list(all_week[(all_week["nielsen_desig_market_area_desc"].isin([t2_market1])) &
                                              (all_week["retailer_name"].isin([[0]])) &
                                              (~all_week["store_division"].isin([store_division_list])) &
                                              (~all_week["store_division"].isin([store_division_list1]))]["store_division"].unique())
        store_division_select_all2=["Select all"]+store_division_list2
        #remove nan
        #store_banner_select_all=[i for i in store_banner_select_all if i==i]
        storeDivisionChoiceInfo2=st.sidebar.multiselect("Please select store numbers", store_division_select_all2,default=["Select all"], key = '3')
        if "Select all" in storeDivisionChoiceInfo2:
            storeDivisionChoiceInfo2=store_division_select_all2[1:]
        store3=storeDivisionChoiceInfo2
        store3=[i for i in store if i!= None]
        
     
           
    
        #Category
        if len(retailer)>1:
            category_list1=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                             (all_hierarchy["retailer_name"].isin([retailer[0]]))]["category"].dropna().unique())
            category_list2=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                             (all_hierarchy["retailer_name"].isin([retailer[1]]))]["category"].dropna().unique())
            category_list = category_list1 +category_list2
            category_list = list(set(category_list))
            if len(category_list) == 0:
                st.write("There is no data present.")
            
        else:
             category_list=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                             (all_hierarchy["retailer_name"].isin([retailer[0]]))]["category"].dropna().unique())
             if len(category_list) == 0:
                st.write("There is no data present.")
        category_list_all=["Select all"]+category_list
        #st.write(category_list_all)
        categoryChoiceInfo=st.sidebar.multiselect("Please select product category", category_list_all,default=["CAT LITTER"])
        if "Select all" in categoryChoiceInfo:
            categoryChoiceInfo=category_list_all[1:]
        category=categoryChoiceInfo
        category=[i for i in category if i!= None]
        
        #product type
        if len(retailer)>1:
            product_list1=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                            (all_hierarchy["retailer_name"].isin([retailer[0]])) & 
                                            (all_hierarchy["category"].isin(categoryChoiceInfo))]["prod_type"].dropna().unique())
            product_list2=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                            (all_hierarchy["retailer_name"].isin([retailer[1]])) & 
                                            (all_hierarchy["category"].isin(categoryChoiceInfo))]["prod_type"].dropna().unique())
            product_list = product_list1 + product_list2
            product_list = list(set(product_list))
            if len(product_list) == 0:
                st.write("There is no data present.")
            
        else:
            product_list=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  
                                            (all_hierarchy["retailer_name"].isin([retailer[0]])) & 
                                            (all_hierarchy["category"].isin(categoryChoiceInfo))]["prod_type"].dropna().unique())
            if len(product_list) == 0:
                st.write("There is no data present.")
        product_list_all=["Select all"]+product_list
        prodTypeChoiceInfo=st.sidebar.multiselect("Please select product types", product_list_all,default=["Select all"])
        if "Select all" in prodTypeChoiceInfo:
            prodTypeChoiceInfo=product_list_all[1:]
        prodType=prodTypeChoiceInfo
        prodType=[i for i in prodType if i!= None]
            
        #target brand
        if len(retailer)>1:
            target_brand_list1=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[0]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo)) ]["brand"].unique() )
            target_brand_list2=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[1]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo)) ]["brand"].unique() )
            target_brand_list = target_brand_list1 + target_brand_list2
            target_brand_list = list(set(target_brand_list))
        else:    
            target_brand_list=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[0]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo)) ]["brand"].unique() )
        target_brand_list_all=["Select all"]+target_brand_list
        #remove nan
        #product_list_all=[i for i in product_list_all if i==i]
        brandChoiceInfo=st.sidebar.multiselect("Please select target brands", target_brand_list_all,default=["FRESH STEP"])
        if "Select all" in brandChoiceInfo:
            brandChoiceInfo=target_brand_list_all[1:]
        target_brand=brandChoiceInfo
        target_brand=[i for i in target_brand if i!= None]
            
        #sub target brand
        if len(retailer)>1:
            sub_target_brand_list1=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[0]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo))& (all_hierarchy["brand"].isin(brandChoiceInfo)) ]["subbrand"].unique() )
            sub_target_brand_list2=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[1]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo))& (all_hierarchy["brand"].isin(brandChoiceInfo)) ]["subbrand"].unique() )
            sub_target_brand_list = sub_target_brand_list1+sub_target_brand_list2
            target_brand_list = list(set())
        else:    
            sub_target_brand_list=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer[0]])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo))& (all_hierarchy["brand"].isin(brandChoiceInfo)) ]["subbrand"].unique() )
        sub_target_brand_list_all=["Select all"]+sub_target_brand_list
        subBrandChoiceInfo=st.sidebar.multiselect("Please select target sub brands", sub_target_brand_list_all,default=["Select all"])
        if "Select all" in subBrandChoiceInfo:
            subBrandChoiceInfo=sub_target_brand_list_all[1:]
        target_sub_brand=subBrandChoiceInfo
        target_sub_brand=[i for i in target_sub_brand if i!= None]
    
        #competitor   
        #sub target brand
        CompbrandChoiceInfo=st.sidebar.multiselect("Please select competitor brands", target_brand_list_all)
        if "Select all" in CompbrandChoiceInfo:
            CompbrandChoiceInfo=target_brand_list_all[1:]
        comp_brand=CompbrandChoiceInfo
        comp_brand=[i for i in comp_brand if i!= None]
        #sub target brand
        comp_sub_target_brand_list=list(all_hierarchy[(all_hierarchy["store_banner"].isin(storeBannerChoiceInfo)) &  (all_hierarchy["retailer_name"].isin([retailer])) & (all_hierarchy["category"].isin(categoryChoiceInfo)) & (all_hierarchy["prod_type"].isin(prodTypeChoiceInfo))& (all_hierarchy["brand"].isin(CompbrandChoiceInfo)) ]["subbrand"].unique() )
        comp_sub_target_brand_list_all=["Select all"]+comp_sub_target_brand_list
        compSubBrandChoiceInfo=st.sidebar.multiselect("Please select competitor sub brands", comp_sub_target_brand_list_all)
        if "Select all" in compSubBrandChoiceInfo:
            compSubBrandChoiceInfo=comp_sub_target_brand_list_all[1:]
        comp_sub_brand=compSubBrandChoiceInfo
        comp_sub_brand=[i for i in comp_sub_brand if i!= None]
         
         
        default_ix_upc=["Yes","No"].index("No")    
        exclude_flag= st.sidebar.selectbox("Please identify UPCs to be excluded", ["Yes","No"],default_ix_upc)
        
        
        #st.write(category)
        #sql query to exclude UPC
        upcFilterSql = """SELECT DISTINCT consumer_upc 
                      FROM %s 
                      WHERE
                        retailer_name IN (%s)
                        AND store_banner IN (%s)
                        AND iri_sub_category IN (%s)
                        AND prod_type IN (%s)
                        AND brand IN (%s)
                        AND subbrand IN (%s)
                    ORDER BY consumer_upc""" % (filter_table_name,(', '.join(["'"+retailer1+"'"])if len([retailer1])>0 else "' '"),
                    (', '.join(["'"+i+"'" for i in store if i!= None])if len(store)>0 else "' '"),
                    (', '.join(["'"+i+"'" for i in category if i!= None])if len(category)>0 else "' '"),
                    (', '.join(["'"+i+"'" for i in prodType if i!= None])if len(prodType)>0 else "' '"),
                    (', '.join(["'"+i+"'" for i in comp_brand if i!= None]+["'"+i+"'" for i in target_brand if i!= None])if len(comp_brand)+len(target_brand)>0 else "' '"),
                    (', '.join(["'"+i+"'" for i in target_sub_brand if i!= None]+["'"+i+"'" for i in comp_sub_brand if i!= None]))if len(comp_sub_brand)+len(target_sub_brand)>0 else "' '")
        #st.write(upcFilterSql)
        excludedUPC=[]
        if exclude_flag=="No":
            excludeUPCUI=None
            excludedUPC=[]
        else:
            #@st.cache(allow_output_mutation=True)
            excludeUPCUI=pd.read_sql(upcFilterSql, cnxn)
            excludedUPC=st.sidebar.multiselect("Please identify UPCs to be excluded", list(excludeUPCUI["consumer_upc"]))
        
            
        #st.write(excludedUPC)
           
        tolerance_of_missing=st.sidebar.number_input("Tolerance",0.15)
        
        group_size=st.sidebar.number_input("Group Size",35)
        
        
        
        #pre_s_week=st.sidebar.selectbox("Pre-test Starting Week", all_week["week_start_date"],index=len(all_week["week_start_date"])-2)
        
        #pre_e_week=st.sidebar.selectbox("Pre-test Ending Week", all_week["week_end_date"],index=len(all_week["week_end_date"])-1)
        
        all_week_start_list=list(all_week[all_week["retailer_name"].isin([retailer1])]["week_start_date"].unique())
        #st.write(all_week_start_list[0].dtypes)
        all_week_start_list = pd.to_datetime(all_week_start_list,format = "%Y-%m-%d")
        all_week_end_list=list(all_week[all_week["retailer_name"].isin([retailer1])]["week_end_date"].unique())
        all_week_end_list = pd.to_datetime(all_week_end_list,format = "%Y-%m-%d")
        pre_s_week=st.sidebar.selectbox("Pre-test Starting Week", all_week_start_list ,index=len(all_week_end_list)-2)
        
        pre_e_week=st.sidebar.selectbox("Pre-test Ending Week", all_week_end_list ,index=len(all_week_end_list)-1)
        
    else:
        all_store_group_names=all_store_group_names['store_group_name'].unique()
        existStores= st.sidebar.selectbox("Please Select the paired store groups you want to use", all_store_group_names)
    
    
    #================== Main Sidebar End=====================
    
    default_ix_random=["Yes","No"].index("No")    
    randomAncova= st.sidebar.selectbox("Perform random ANCOVA?", ["Yes","No"],default_ix_random)
    
    #action = True
        
    
        
    ########## Store Group Start #######################
    qap_input=pd.DataFrame([])
    
    #if (action==True) & (existFlag=="No") & (selection == 'Store Groups'):
    if (existFlag=="No") & (selection == 'Store Groups'):   
        # Translate starting date to week num
        pre_s_week_int =  all_week[(all_week["week_start_date"] == pre_s_week)]["week_number"].unique()[0]
        pre_e_week_int =  all_week[(all_week["week_end_date"] == pre_e_week)]["week_number"].unique()[0]
        
        # Translate store_banner into div number because of the quotation issue
        div_nbrs = (all_hierarchy[all_hierarchy["store_banner"].isin(store)]["div_nbr"]).unique()
        #st.write(div_nbrs)
        #div_nbrs1 = div_nbrs.tolist()
        
        #div_nbrs = (all_hierarchy[all_hierarchy["retailer_name"].isin(store)]["div_nbr"]).unique()
        if len(div_nbrs) == 1 and div_nbrs == None:
            div_nbrs = []
            #st.write(div_nbrs)
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_markets_str=(', '.join(["'"+i+"'" for i in ct_market if i!= None]+["'"+i+"'" for i in t1_market if i!= None]+["'"+i+"'" for i in t2_market if i!= None])if len(ct_market+t2_market+t1_market)>0 else "' '")
        all_retailers_str=(', '.join(["'"+retailer1+"'"])if len([retailer1])>0 else "' '")
        all_category_str=(', '.join(["'"+i+"'" for i in category if i!= None])if len(category)>0 else "' '")
        all_prodtype_str=(', '.join(["'"+i+"'" for i in prodType if i!= None])if len(prodType)>0 else "' '")
        all_div_nbr_str=(', '.join(["'"+i+"'" for i in div_nbrs if i!= None])if len(div_nbrs)>0 else "'NA'")
        
        if all_div_nbr_str == ' ':
            all_div_nbr_str = 'NA'
            
        #st.write(all_div_nbr_str)
        
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None]+["'"+i+"'" for i in comp_brand if i!= None])if len(target_brand+comp_brand)>0 else "' '")
        all_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None]+["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand+target_sub_brand)>0 else "' '")
        all_exclude_upc=(', '.join(["'"+i+"'" for i in excludedUPC if i!= None])if len(excludedUPC)>0 else "' '")
        comp_brands_str=(', '.join(["'"+i+"'" for i in comp_brand if i!= None])if len(comp_brand)>0 else "' '")
        target_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None])if len(target_sub_brand)>0 else "' '")
        comp_subbrands_str=(', '.join(["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand)>0 else "' '")
        all_store_str=(', '.join(["'"+i+"'" for i in store if i!= None])if len(store)>0 else "' '")
        
          
        string1 = """SELECT a.MarketC,
                              a.MarketDes,
    	                      a.store_division,
    	                      a.week_number,
    	                      a.week_start_date,
    	                      a.week_end_date,
    	                      sum(a.target_upc_count) as target_upc_count,
    	                      sum(a.target_stat_case_vol) as target_stat_case_vol,
    	                      sum(a.target_avg_price) as target_avg_price,
    	                      sum(a.comp_upc_count) as comp_upc_count,
    	                      sum(a.comp_stat_case_vol) as comp_stat_case_vol,
    	                      sum(a.comp_avg_price) as comp_avg_price
                       FROM 
                      (SELECT nielsen_desig_market_area_cd as MarketC, 
                              nielsen_desig_market_area_desc as MarketDes,         
                              store_division,            
                              week_number,           
                              week_start_date,           
                              week_end_date,       
                             (case when brand in (%s) then COUNT(distinct consumer_upc) else 0 end) as target_upc_count,
                             (case when brand in (%s) then SUM(stat_case_volume) else 0 end) as target_stat_case_vol,   
                             (case when brand in (%s) then AVG(average_price_per_stat_case_volume) else 0 end) as target_avg_price,
                             (case when brand not in (%s) THEN  COUNT(distinct consumer_upc) else 0 end) AS comp_upc_count,
                             (case when brand not in (%s) THEN  sum(stat_case_volume) else 0 end) AS comp_stat_case_vol,
                             (case when brand not in (%s) THEN  AVG(average_price_per_stat_case_volume) else 0 end) AS comp_avg_price  
                       FROM %s        
                       WHERE week_number >= %i      
                       AND week_number <= %i       
                       AND retailer_name in (%s)       
                       AND div_nbr in (%s)         
    				   AND iri_sub_category in (%s)         
    				   AND prod_type in (%s)         
                       AND brand in (%s)        
                       AND subbrand in (%s)       
                       AND nielsen_desig_market_area_desc in (%s)       
                       AND consumer_upc not in (%s)      
                       GROUP BY   nielsen_desig_market_area_desc,      
                                  nielsen_desig_market_area_cd, 
    							  store_division, 
    							  week_number, 
    							  week_start_date, 
    							  week_end_date,
    							  brand   
                       HAVING nielsen_desig_market_area_cd is not null) a
                       GROUP BY a.MarketC,
                                a.MarketDes,
    	                        a.store_division,
    	                        a.week_number,
    	                        a.week_start_date,
    	                        a.week_end_date
                       HAVING sum(target_upc_count) > 0""" % (target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,
                            dev_table_name,int(pre_s_week_int), int(pre_e_week_int),all_retailers_str,all_div_nbr_str, all_category_str, all_prodtype_str,
                            all_brands_str, all_subbrands_str,all_markets_str,all_exclude_upc)                       
        
        qap_input = qap_input.append(pd.read_sql(string1, cnxn))
        qap_input.to_csv("qap_input.csv")
        #st.write(qap_input)
         
        
        if len(username) == 0:
            aut = 'local server'
        else:
            aut = username
        
             
        df=pd.DataFrame()
        df['date_created']= [date.today()]
        #df['date_created']=df['date_created'].astype('datetime64[ns]')
        df['author']= [aut]
        
        
        df['all_retailers_str']=[retailer]
        df['all_store_str']=all_store_str
        df['all_category_str']=all_category_str
        df['all_prodtype_str']=all_prodtype_str
        df['all_div_nbr_str'] = all_div_nbr_str
        df['target_brands_str']=target_brands_str
        df['comp_brands_str']=comp_brands_str
        df['target_subbrands_str']=target_subbrands_str    
        df['comp_subbrands_str']=comp_subbrands_str
        df['tolerance_of_missing']=tolerance_of_missing
        df['all_exclude_upc']=all_exclude_upc
        df['Test1_Market']=(', '.join(["'"+i+"'" for i in t1_market if i!= None])if len(t1_market)>0 else "' '")
        df['Test2_Market']=(', '.join(["'"+i+"'" for i in t2_market if i!= None])if len(t2_market)>0 else "' '")
        df['Control_Market']=(', '.join(["'"+i+"'" for i in ct_market if i!= None])if len(ct_market)>0 else "' '")
        
        
        
        
        df6 = df['Control_Market'] + df['Test1_Market'] + df['Test2_Market']
        df6 = df6.iloc[0]
        df6 = df6.replace("''", ",")
        df6 = df6.replace("'", "")
        df6 = df6.split(",")
       
        df3 = df['Control_Market'].iloc[0]
        df3_control = list(df3.split(","))
        
        df4 = df['Test1_Market'].iloc[0]
        df3_test1 = list(df4.split(","))
        
        df5 = df['Test2_Market'].iloc[0]
        df3_test2 = list(df5.split(","))
        
       
        
        
        df = pd.concat([df]*len(qap_input), ignore_index=True)
            
        df1 = pd.concat([df, qap_input], axis=1)
        df1.rename(columns = {'MarketC' : 'marketc', 'MarketDes' : 'marketdes'}, inplace = True)
        df1 = df1.fillna(value=0)
        #st.write(repr(df1["marketdes"].iloc[0]))
        #df2 = df1['marketdes'].tolist()
        
        rating =[]
        rat = []
        rat_low = []
        for j in range(len(df1["marketdes"])):
            
            if repr(df1["marketdes"].iloc[j]) in df3_control:
               rating.append('Control')
               rat.append('3')
               rat_low.append('control')
                
            elif repr(df1["marketdes"].iloc[j]) in df3_test1:
               rating.append('Test1')
               rat.append('1') 
               rat_low.append('test1')
            elif repr(df1["marketdes"].iloc[j]) in df3_test2:
                rating.append('Test2')
                rat.append('2')
                rat_low.append('test2')
            else:
                rating.append('NULL') 
                rat.append('NULL')
                rat_low.append('NULL')
            
        df1['mk'] = rating
        df1['group_number'] = rat
        df1['group_name']= rat_low
      
        #user_input = st.text_input("Enter Store Group Name")
        
        store_group_name = 'test4'
        new1 = []
        for j in range(len(df1["marketdes"])):
            if df1["marketdes"].iloc[j] in df6:
                new1.append(store_group_name)
            else:
                new1.append('NULL')
                   
        df1['store_group_name'] = new1
        df1['author'] = df1['author'].astype(str)
        df1['all_retailers_str'] = df1['all_retailers_str'].astype('str')
        df1['all_store_str']= df1['all_store_str'].astype('str')
        
        #st.write(df1)
        df1.to_csv("df1.csv")
        
       
         #state = SessionState.get()
         #if st.button("Next"):
              #next(state.frames)
              #st.write(state.frames)
       
            
            
        #form = st.form(key='my-form')
        #name = form.text_input('Enter your name')
        #submit = form.form_submit_button('Submit')
        
        #st.write('Press submit to have your name printed below')
        
        #if submit:
            #st.write(f'hello {name}')
            
         
        #temp = []
        #for index, row in df1.iterrows():
            #temp.append((row.all_retailers_str, row.mk, row.group_number, row.group_name, row.store_group_name, row.author, row.all_store_str,row.all_category_str, row.all_prodtype_str, row.all_div_nbr_str, row.target_brands_str, row.comp_brands_str, row.target_subbrands_str, row.comp_subbrands_str, row.tolerance_of_missing, row.all_exclude_upc, row.marketc, row.marketdes, row.store_division, row.week_number, row.week_start_date, row.target_upc_count, row.target_stat_case_vol, row.target_avg_price, row.comp_upc_count, row.comp_stat_case_vol,row.comp_avg_price))
            
        #q= """SET QUOTED_IDENTIFIER ON insert into qap.storegroups(all_retailers_str, mk, group_number, group_name, store_group_name,author, all_store_str,all_category_str, all_prodtype_str, all_div_nbr_str, target_brands_str, comp_brands_str, target_subbrands_str, comp_subbrands_str, tolerance_of_missing, all_exclude_upc, marketc, marketdes, store_division, week_number, week_start_date, target_upc_count, target_stat_case_vol, target_avg_price, comp_upc_count, comp_stat_case_vol,comp_avg_price ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        #cursor.executemany(q, temp)
        #cnxn.commit()
        #cursor.close()
        
        
        store_group_no=page.app(qap_input,df1,ct_market,t2_market,t1_market,all_week,tolerance_of_missing,group_size,'No')   
        
    #if((action==True) & (existFlag=="Yes")& (selection == 'Store Groups')):
    if((existFlag=="Yes")& (selection == 'Store Groups')):     
        
        query = "SELECT * FROM "+ store_groups_table_name+ " WHERE store_group_name ='" + existStores+"'"
        groupedStores = pd.read_sql(query, cnxn)
        #st.write(groupedStores)
        ct_market = groupedStores[groupedStores["group_name"] == "control"]["marketdes"].unique()
        t1_market = groupedStores[groupedStores["group_name"] == "test1"]["marketdes"].unique()
        t2_market = groupedStores[groupedStores["group_name"] == "test2"]["marketdes"].unique()
        tolerance_of_missing = float(groupedStores["tolerance_of_missing"][0])
        #all_week = None
        #group_size=group_size
        group_size=None
    
        df1=pd.read_csv("df1.csv")
        store_group_yes=page.app(groupedStores,df1,ct_market,t2_market,t1_market,all_week,tolerance_of_missing,group_size,'Yes')
        #st.write(store_group_yes)


        
        
    ########## Store Group End #######################
    
    #st.write(qap_input)
####========================= In Market Tracking Start ===========================

    if ((existFlag=="No") & (selection == 'In Market Tracking')) :
        pre_s_week_int =  all_week[(all_week["week_start_date"] == pre_s_week)]["week_number"].unique()[0]
        pre_e_week_int =  all_week[(all_week["week_end_date"] == pre_e_week)]["week_number"].unique()[0]
        
        # Translate store_banner into div number because of the quotation issue
        div_nbrs = (all_hierarchy[all_hierarchy["store_banner"].isin(store)]["div_nbr"]).unique()   
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_markets_str=(', '.join(["'"+i+"'" for i in ct_market if i!= None]+["'"+i+"'" for i in t1_market if i!= None]+["'"+i+"'" for i in t2_market if i!= None])if len(ct_market+t2_market+t1_market)>0 else "' '")
        all_retailers_str=(', '.join(["'"+retailer1+"'"])if len([retailer1])>0 else "' '")
        all_category_str=(', '.join(["'"+i+"'" for i in category if i!= None])if len(category)>0 else "' '")
        all_prodtype_str=(', '.join(["'"+i+"'" for i in prodType if i!= None])if len(prodType)>0 else "' '")
        all_div_nbr_str=(', '.join(["'"+i+"'" for i in div_nbrs if i!= None])if len(div_nbrs)>0 else "' '")
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None]+["'"+i+"'" for i in comp_brand if i!= None])if len(target_brand+comp_brand)>0 else "' '")
        all_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None]+["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand+target_sub_brand)>0 else "' '")
        all_exclude_upc=(', '.join(["'"+i+"'" for i in excludedUPC if i!= None])if len(excludedUPC)>0 else "' '")
        comp_brands_str=(', '.join(["'"+i+"'" for i in comp_brand if i!= None])if len(comp_brand)>0 else "' '")
        target_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None])if len(target_sub_brand)>0 else "' '")
        comp_subbrands_str=(', '.join(["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand)>0 else "' '")
        all_store_str=(', '.join(["'"+i+"'" for i in store if i!= None])if len(store)>0 else "' '")
        
          
        string1 = """SELECT a.MarketC,
                              a.MarketDes,
    	                      a.store_division,
    	                      a.week_number,
    	                      a.week_start_date,
    	                      a.week_end_date,
    	                      sum(a.target_upc_count) as target_upc_count,
    	                      sum(a.target_stat_case_vol) as target_stat_case_vol,
    	                      sum(a.target_avg_price) as target_avg_price,
    	                      sum(a.comp_upc_count) as comp_upc_count,
    	                      sum(a.comp_stat_case_vol) as comp_stat_case_vol,
    	                      sum(a.comp_avg_price) as comp_avg_price
                       FROM 
                      (SELECT nielsen_desig_market_area_cd as MarketC, 
                              nielsen_desig_market_area_desc as MarketDes,         
                              store_division,            
                              week_number,           
                              week_start_date,           
                              week_end_date,       
                             (case when brand in (%s) then COUNT(distinct consumer_upc) else 0 end) as target_upc_count,
                             (case when brand in (%s) then SUM(stat_case_volume) else 0 end) as target_stat_case_vol,   
                             (case when brand in (%s) then AVG(average_price_per_stat_case_volume) else 0 end) as target_avg_price,
                             (case when brand not in (%s) THEN  COUNT(distinct consumer_upc) else 0 end) AS comp_upc_count,
                             (case when brand not in (%s) THEN  sum(stat_case_volume) else 0 end) AS comp_stat_case_vol,
                             (case when brand not in (%s) THEN  AVG(average_price_per_stat_case_volume) else 0 end) AS comp_avg_price  
                       FROM %s        
                       WHERE week_number >= %i      
                       AND week_number <= %i       
                       AND retailer_name in (%s)       
                       AND div_nbr in (%s)         
    				   AND iri_sub_category in (%s)         
    				   AND prod_type in (%s)         
                       AND brand in (%s)        
                       AND subbrand in (%s)       
                       AND nielsen_desig_market_area_desc in (%s)       
                       AND consumer_upc not in (%s)      
                       GROUP BY   nielsen_desig_market_area_desc,      
                                  nielsen_desig_market_area_cd, 
    							  store_division, 
    							  week_number, 
    							  week_start_date, 
    							  week_end_date,
    							  brand   
                       HAVING nielsen_desig_market_area_cd is not null) a
                       GROUP BY a.MarketC,
                                a.MarketDes,
    	                        a.store_division,
    	                        a.week_number,
    	                        a.week_start_date,
    	                        a.week_end_date
                       HAVING sum(target_upc_count) > 0""" % (target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,
                            dev_table_name,int(pre_s_week_int), int(pre_e_week_int),all_retailers_str,all_div_nbr_str, all_category_str, all_prodtype_str,
                            all_brands_str, all_subbrands_str,all_markets_str,all_exclude_upc)                       
        
        qap_input = qap_input.append(pd.read_sql(string1, cnxn))
        #df=df.append(qap_input)
        
        col1,col2, col3 = st.beta_columns(3)
        
        default_ix_yn=53
        test_s_week= col1.selectbox("Test Start", list(all_week["week_start_date"]),index=default_ix_yn)
    
        
        test_length= col2.number_input("Test Duration (Number of Weeks)",0,1000,13)
       
        default_ix_zn=1
        option= col3.selectbox("Reference Period", list(["52 Weeks before test","13 Weeks before test","Same weeks as test Last Year"]),index=default_ix_zn)
    
         
        #sentence = st.button('Get plots')
    
       # if sentence:
            #st.write(("PLOT WILL BE DISPLAYED HERE"))
       
        test_s_week = pd.to_datetime(test_s_week,format="%Y-%m-%d")
          # First get the end weeks (This end week needs to be excluded (<))
        test_e_week = pd.to_datetime(test_s_week ,format="%Y-%m-%d") + timedelta(int(test_length) * 7)
        
        if option == '13 Weeks before test':
           
            ref_s_week = test_s_week - timedelta(13 * 7)
           
            ref_e_week = test_s_week
           
          
        elif(option == 'Same Weeks as test Last Year'):
            ref_s_week  = test_s_week -timedelta( 52*7)
           
            ref_e_week = test_e_week -timedelta( 52*7)
           
        else:
            ref_s_week = test_s_week -timedelta( 52 * 7)
           
            ref_e_week = test_s_week
            
        #st.write(test_s_week)
        #st.write(test_e_week)
        #st.write(ref_s_week)
        #st.write(ref_e_week)
        
        min_val = min(all_week["week_start_date"])
        min_val1 = all_week[all_week["week_start_date"] == test_s_week]["week_number"]
        min_val2 = all_week[all_week["week_start_date"] == min_val]["week_number"]
        if test_s_week >= min_val:
            test_s_week_int = min_val1
            test_s_week_int = test_s_week_int.iloc[0]
        else:
            test_s_week_int = min_val2
            test_s_week_int = test_s_week_int.iloc[0]
            
        max_val = max(all_week["week_start_date"])
        max_val1 = all_week[all_week["week_start_date"] == test_e_week]["week_number"]
        max_val2 = all_week[all_week["week_start_date"] == max_val]["week_number"]
        #st.write(max_val2)
        #st.write(test_s_week_int)
        if test_e_week <= max_val:
            test_e_week_int = max_val1
            test_e_week_int = test_e_week_int.iloc[0]
        else:
            test_e_week_int = max_val2
            test_e_week_int = test_e_week_int.iloc[0]
        #st.write(test_e_week_int)    
        ref_min_val = min(all_week["week_start_date"])
        ref_min_val1 = all_week[all_week["week_start_date"] == ref_s_week]["week_number"]
        ref_min_val2 = all_week[all_week["week_start_date"] == min_val]["week_number"]
        if ref_s_week >= ref_min_val:
            ref_s_week_int = ref_min_val1
            
            ref_s_week_int= ref_s_week_int.iloc[0]
        else:
            ref_s_week_int = ref_min_val2
            
            ref_s_week_int = ref_s_week_int.iloc[0]
        
        ref_max_val = max(all_week["week_start_date"])
        ref_max_val1 = all_week[all_week["week_start_date"] == ref_e_week]["week_number"]
        ref_max_val2 = all_week[all_week["week_start_date"] == min_val]["week_number"]
        if ref_e_week <= ref_max_val:
            ref_e_week_int = ref_max_val1
            
            ref_e_week_int = ref_e_week_int.iloc[0]
        else:
            ref_e_week_int = ref_max_val2
            
            ref_e_week_int = ref_e_week_int.iloc[0]
       
            
        # Translate starting date to week num
        pre_s_week_int =  all_week[(all_week["week_start_date"] == pre_s_week)]["week_number"].unique()[0]
        pre_e_week_int =  all_week[(all_week["week_end_date"] == pre_e_week)]["week_number"].unique()[0]
        
        
        
        
        GroupedStores=Store_Groups.app1(qap_input,ct_market,t2_market,t1_market,all_week,tolerance_of_missing,group_size,'No')
        GroupedStores_sub = GroupedStores.loc[0:group_size]
        
        div_nbrs = (all_hierarchy[all_hierarchy["store_banner"].isin(store)]["div_nbr"]).unique()  
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_markets_str=(', '.join(["'"+i+"'" for i in ct_market if i!= None]+["'"+i+"'" for i in t1_market if i!= None]+["'"+i+"'" for i in t2_market if i!= None])if len(ct_market+t2_market+t1_market)>0 else "' '")
        all_retailers_str=(', '.join(["'"+retailer1+"'"])if len([retailer1])>0 else "' '")
        all_category_str=(', '.join(["'"+i+"'" for i in category if i!= None])if len(category)>0 else "' '")
        all_prodtype_str=(', '.join(["'"+i+"'" for i in prodType if i!= None])if len(prodType)>0 else "' '")
        all_div_nbr_str=(', '.join(["'"+i+"'" for i in div_nbrs if i!= None])if len(div_nbrs)>0 else "' '")
        target_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None])if len(target_brand)>0 else "' '")
        all_brands_str=(', '.join(["'"+i+"'" for i in target_brand if i!= None]+["'"+i+"'" for i in comp_brand if i!= None])if len(target_brand+comp_brand)>0 else "' '")
        all_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None]+["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand+target_sub_brand)>0 else "' '")
        all_exclude_upc=(', '.join(["'"+i+"'" for i in excludedUPC if i!= None])if len(excludedUPC)>0 else "' '")
        comp_brands_str=(', '.join(["'"+i+"'" for i in comp_brand if i!= None])if len(comp_brand)>0 else "' '")
        target_subbrands_str=(', '.join(["'"+i+"'" for i in target_sub_brand if i!= None])if len(target_sub_brand)>0 else "' '")
        comp_subbrands_str=(', '.join(["'"+i+"'" for i in comp_sub_brand if i!= None])if len(comp_sub_brand)>0 else "' '")
        #all_stores_str = paste("'",c(GroupedStores_sub$store1, GroupedStores_sub$store2, GroupedStores_sub$store3),"'",sep = '', collapse = ', ')
        #all_store_str=(', '.join(["'"+i+"'" for i in store if i!= None])if len(store)>0 else "' '")
        all_store_str=(', '.join(["'"+i+"'" for i in GroupedStores_sub.store1 if i!= None]+["'"+i+"'" for i in GroupedStores_sub.store2 if i!= None]+["'"+i+"'" for i in GroupedStores_sub.store3 if i!= None])if len(GroupedStores_sub.store1+GroupedStores_sub.store2+GroupedStores_sub.store3)>0 else "' '")
        
        string1 = """SELECT a.MarketC,
                              a.MarketDes,
    	                      a.store_division,
    	                      a.week_number,
    	                      a.week_start_date,
    	                      a.week_end_date,
    	                      sum(a.target_upc_count) as target_upc_count,
    	                      sum(a.target_stat_case_vol) as target_stat_case_vol,
    	                      sum(a.target_avg_price) as target_avg_price,
    	                      sum(a.comp_upc_count) as comp_upc_count,
    	                      sum(a.comp_stat_case_vol) as comp_stat_case_vol,
    	                      sum(a.comp_avg_price) as comp_avg_price
                       FROM 
                      (SELECT nielsen_desig_market_area_cd as MarketC, 
                              nielsen_desig_market_area_desc as MarketDes,         
                              store_division,            
                              week_number,           
                              week_start_date,           
                              week_end_date,       
                             (case when brand in (%s) then COUNT(distinct consumer_upc) else 0 end) as target_upc_count,
                             (case when brand in (%s) then SUM(stat_case_volume) else 0 end) as target_stat_case_vol,   
                             (case when brand in (%s) then AVG(average_price_per_stat_case_volume) else 0 end) as target_avg_price,
                             (case when brand not in (%s) THEN  COUNT(distinct consumer_upc) else 0 end) AS comp_upc_count,
                             (case when brand not in (%s) THEN  sum(stat_case_volume) else 0 end) AS comp_stat_case_vol,
                             (case when brand not in (%s) THEN  AVG(average_price_per_stat_case_volume) else 0 end) AS comp_avg_price  
                       FROM %s        
                       WHERE week_number >= %i      
                       AND week_number <= %i       
                       AND retailer_name in (%s)       
                       AND div_nbr in (%s)         
    				   AND iri_sub_category in (%s)         
    				   AND prod_type in (%s)         
                       AND brand in (%s)        
                       AND subbrand in (%s)       
                       AND nielsen_desig_market_area_desc in (%s)       
                       AND consumer_upc not in (%s)      
                       GROUP BY   nielsen_desig_market_area_desc,      
                                  nielsen_desig_market_area_cd, 
    							  store_division, 
    							  week_number, 
    							  week_start_date, 
    							  week_end_date,
    							  brand   
                       HAVING nielsen_desig_market_area_cd is not null) a
                       GROUP BY a.MarketC,
                                a.MarketDes,
    	                        a.store_division,
    	                        a.week_number,
    	                        a.week_start_date,
    	                        a.week_end_date
                       HAVING sum(target_upc_count) > 0""" % (target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,
                            dev_table_name,int(pre_s_week_int), int(pre_e_week_int),all_retailers_str,all_div_nbr_str, all_category_str, all_prodtype_str,
                            all_brands_str, all_subbrands_str,all_markets_str,all_exclude_upc)  
        
        qap_input=pd.read_sql(string1, cnxn)
        qap_input.loc[qap_input["MarketDes"].isin(ct_market), 'mk'] = 'Control'
        qap_input.loc[qap_input["MarketDes"].isin(t1_market), 'mk'] = 'Test1'
        qap_input.loc[qap_input["MarketDes"].isin(t2_market), 'mk'] = 'Test2'
       
        # label the test and control markets
        #labels_tb = qap_input[["store_division","mk"]].unique()
        labels_tb=qap_input[["store_division","mk"]].drop_duplicates()
        labels_tb=labels_tb.rename(columns={'mk':'group_name'})
        #st.write(labels_tb)
        
        string2 = """SELECT a.MarketC,
                              a.MarketDes,
    	                      a.store_division,
    	                      a.week_number,
    	                      a.week_start_date,
    	                      a.week_end_date,
    	                      sum(a.target_upc_count) as target_upc_count,
    	                      sum(a.target_stat_case_vol) as target_stat_case_vol,
    	                      sum(a.target_avg_price) as target_avg_price,
    	                      sum(a.comp_upc_count) as comp_upc_count,
    	                      sum(a.comp_stat_case_vol) as comp_stat_case_vol,
    	                      sum(a.comp_avg_price) as comp_avg_price
                       FROM 
                      (SELECT nielsen_desig_market_area_cd as MarketC, 
                              nielsen_desig_market_area_desc as MarketDes,         
                              store_division,            
                              week_number,           
                              week_start_date,           
                              week_end_date,       
                             (case when brand in (%s) then COUNT(distinct consumer_upc) else 0 end) as target_upc_count,
                             (case when brand in (%s) then SUM(stat_case_volume) else 0 end) as target_stat_case_vol,   
                             (case when brand in (%s) then AVG(average_price_per_stat_case_volume) else 0 end) as target_avg_price,
                             (case when brand not in (%s) THEN  COUNT(distinct consumer_upc) else 0 end) AS comp_upc_count,
                             (case when brand not in (%s) THEN  sum(stat_case_volume) else 0 end) AS comp_stat_case_vol,
                             (case when brand not in (%s) THEN  AVG(average_price_per_stat_case_volume) else 0 end) AS comp_avg_price  
                       FROM %s        
                       WHERE week_number >= %i      
                       AND week_number <= %i       
                       AND retailer_name in (%s)       
                       AND div_nbr in (%s)         
    				   AND iri_sub_category in (%s)         
    				   AND prod_type in (%s)         
                       AND brand in (%s)        
                       AND subbrand in (%s)       
                       AND nielsen_desig_market_area_desc in (%s)
                       AND store_division in (%s)
                       AND consumer_upc not in (%s)      
                       GROUP BY   nielsen_desig_market_area_desc,      
                                  nielsen_desig_market_area_cd, 
    							  store_division, 
    							  week_number, 
    							  week_start_date, 
    							  week_end_date,
    							  brand   
                       HAVING nielsen_desig_market_area_cd is not null) a
                       GROUP BY a.MarketC,
                                a.MarketDes,
    	                        a.store_division,
    	                        a.week_number,
    	                        a.week_start_date,
    	                        a.week_end_date
                       HAVING sum(target_upc_count) > 0""" 
                       
                       
        query_test = string2 % (target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,
                            dev_table_name,int(test_s_week_int),int(test_e_week_int),all_retailers_str,all_div_nbr_str, all_category_str, all_prodtype_str,
                            all_brands_str, all_subbrands_str,all_markets_str,all_store_str,all_exclude_upc)
        #st.write(query_test)                   
    
        tb_in_test=pd.read_sql(query_test, cnxn)
        tb_in_test = tb_in_test .merge(labels_tb,on='store_division',how='left')
              
        
        query_ref = string2 % (target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,target_brands_str,dev_table_name,
                                int(ref_s_week_int), int(ref_e_week_int),all_retailers_str,all_div_nbr_str, all_category_str, all_prodtype_str,
                                all_brands_str, all_subbrands_str,all_markets_str,all_store_str,all_exclude_upc)
                
        #GCP#tb_ref = query_exec(query_ref, project = projectName, use_legacy_sql = F, max_pages = Inf)
        tb_ref = pd.read_sql(query_ref, cnxn)
        tb_ref = tb_ref.merge(labels_tb,on='store_division',how='left')
        #st.write(tb_in_test)
        #st.write(tb_ref)  
    
        errMsg1 = "No data found Please select a valid time range or load in more data"
        errMsg2 = "No data found in testing period. Please select a valid time range or load in more data"    
        
        if len(tb_ref) > 0:
            tracking_error_msg_1='reference period input validated'
        else:
            tracking_error_msg_1=errMsg1
            
        if len(tb_in_test) >0:
            tracking_error_msg_2='in test period input validated'
        else:
            tracking_error_msg_2=errMsg2
        
            
        #st.write(tb_in_test)
        tb_in_test["period"] = 'Testing'
        tb_ref["period"] = 'Reference'
        tb_comb = tb_in_test.append(tb_ref)
        
        tb_comb_gathered = tb_comb[['target_stat_case_vol', 'target_avg_price', 'target_upc_count', 'comp_upc_count', 'comp_stat_case_vol','comp_avg_price', 'group_name', 'period']].groupby(['period','group_name']).mean().reset_index()
          
        tb_comb_gathered=tb_comb_gathered.rename(columns={'target_stat_case_vol':'stat_case_vol','target_avg_price':'price_per_scv',
                                                   'comp_avg_price':'comp_price_per_scv'       })
        
        tb_comb_gathered=pd.melt(tb_comb_gathered, id_vars=['period','group_name'],value_vars=['stat_case_vol', 'price_per_scv', 'target_upc_count',
                                                            'comp_upc_count', 'comp_stat_case_vol', 'comp_price_per_scv'])
       
       
        #st.write(tb_comb_gathered)            
          
                                           
         # The Dynamic plots for Stat Case Volume
        scv_in_test = tb_in_test[['week_start_date','target_stat_case_vol','group_name']]
        scv_ref = tb_ref[['week_start_date','target_stat_case_vol','group_name']]
        
        scv_in_test['period'] = 'Testing'
        scv_ref['period'] = 'Reference'
        scv_tb = scv_in_test.append(scv_ref)
      
        test_start_date = min(scv_in_test['week_start_date'])
        
        scv_mk = tb_in_test['group_name'].unique()
        #st.write(scv_mk)
        st.title('Stat Case Volume Tracking')
        
        temp1 = ["raw", "smoothed"]
        smooth_flag = st.selectbox("Smooth_flag", temp1)
        
        if option == 'Same Weeks as test Last Year' and smooth_flag == 'raw':
            
            scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
            
            scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='linear'))
            st.plotly_chart(scv_track_fig)
            
        elif option == 'Same Weeks as test Last Year' and smooth_flag == 'smoothed':
            
            scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
            
            scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='spline'))
            st.plotly_chart(scv_track_fig)  
            
        elif option == '13 Weeks before test' and smooth_flag == 'raw':
            
            scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
            
            scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='linear'))
            st.plotly_chart(scv_track_fig) 
        
        elif option == '13 Weeks before test' and smooth_flag == 'smoothed':
            
            scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
            
            scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='spline'))
            st.plotly_chart(scv_track_fig) 
            
        elif option == '52 Weeks before test' and smooth_flag == 'raw':
            
            scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
            
            scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='linear'))
            st.plotly_chart(scv_track_fig)      
            
        else:
              scv_track = scv_tb[scv_tb['group_name'].isin(scv_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
              scv_track = scv_track.rename(columns={'target_stat_case_vol': 'ag_scv'})
              
              scv_track_fig = px.line(scv_track, x='week_start_date', y='ag_scv', color='group_name',labels={
                             "ag_scv": "Average Stat Case Volume",
                             "week_start_date": "Date" })
              scv_track_fig=scv_track_fig.update_traces(mode='markers+lines', line=dict(shape='spline'))
              st.plotly_chart(scv_track_fig)                                            
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
            
        # The Comparison Bar Panel
        
        
        
        st.title('Covariates Tracking')
        temp = tb_in_test[tb_in_test.columns[~tb_in_test.columns.isin(['MarketC','MarketDes', 'target_stat_case_vol','store_division','week_number','period','week_start_date','week_end_date', 'group_name'])]]    
        temp=temp.columns
        #st.write(temp)
        
        cov_selection = st.selectbox("Covariates", temp)
        cov_in_test =tb_in_test[['week_start_date',cov_selection,'group_name']]
        cov_ref =tb_ref[['week_start_date',cov_selection,'group_name']]  
        
        cov_in_test['period'] = 'Testing'
        cov_ref['period']= 'Reference'
        cov_tb = cov_in_test.append(cov_ref)
        #st.write(cov_tb)
        
        
        cov_tb = cov_tb.rename(columns={ cov_selection : "selcted_cov"})
        test_start_date = min(cov_in_test['week_start_date'])
        cov_mk = tb_in_test['group_name'].unique()
        temp2 = ["raw", "smoothed"]
        smooth_flag_cov = st.selectbox("Smooth_flag_cov", temp2)
        rating = st.radio('Groups',('Control','Test1'))
        
        if option == 'Same Weeks as test Last Year' and smooth_flag_cov == 'raw':
            
            cov_track = cov_tb[cov_tb['group_name'].isin(cov_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            cov_track = cov_track.rename(columns={'selcted_cov': 'ag_cov'})
            
            
            cov_track_fig = px.line(cov_track, x='week_start_date', y='ag_cov', color='group_name',labels={
                             "ag_cov": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            cov_track_fig=cov_track_fig.update_traces(mode='markers+lines', line=dict(shape='linear'))
            st.plotly_chart(cov_track_fig)
            
        else:
            
            cov_track = cov_tb[cov_tb['group_name'].isin(cov_mk)].groupby(['period', 'group_name', 'week_start_date']).mean().reset_index()
            cov_track = cov_track.rename(columns={'selcted_cov': 'ag_cov'})
           
            cov_track_fig = px.line(cov_track, x='week_start_date', y='ag_cov', color='group_name',labels={
                             "ag_cov": "Average Stat Case Volume",
                             "week_start_date": "Date" })
            cov_track_fig=cov_track_fig.update_traces(mode='markers+lines', line=dict(shape='spline'))
            st.plotly_chart(cov_track_fig) 
        
        
        
        
        
        
        
                
       
        
        
        
    
if __name__ == '__main__':
    if st._is_running_with_streamlit:
        main()
    else:
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())
