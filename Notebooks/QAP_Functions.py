# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 00:59:55 2021

@author: sourabh.ghosh
"""
import numpy as np

from dtw import *
import pandas as pd
from statistics import *
import streamlit as st

def dissimilarity(vec1,vec2):
  #'This function calculates the dissimilarity between two time series 
  #'The dissimilarity is calculated by Dynamic Time Warping (DTW)
  #'
  #'----- Parameters:
  #'vec1: the first numeric vector
  #'vec2: the second numeric vector of the same length as vec1
  #'
  #'----- Return:
  #'output: the normalized distance calculated with DTW
  #'
  #'
  
  #vec1 = [float(i) for i in vec1] 
  #vec2 = [float(i) for i in vec2]
  

  output = dtw(vec1,vec2).distance
  
  return(output)


dissimilarity(1,2)


def dissimilarity_1(vec1,vec2, wts=[0.1,0.1,0.8]):
  #' This function calculate the dissimilarity between two vectors based on three factors: 1. difference
  vec1 = float(vec1)
  vec2 = float(vec2)
  
  mn1 = mean([vec1])
  mn2 = mean([vec2])
  
  var1 = np.var(vec1)
  var2 = np.var(vec2)
  
  #Measurments in difference in variance
  diff_in_variance = abs((var1 - var2)/var1)
  
  pen = 0
  
  pen = pow((1 + ((abs((abs(mn1 - mn2)/mn1))))),3)
  
  def nwsmoothing(v):
      smoothed = []
      for i in range(1,len(v)):
          w = 1/(pow(np.array(list(range(1, len(v))))-1,4)+1)
          smoothed = [smoothed,np.sum(np.dot(w,v))/np.sum(w)]  
      return(smoothed)
  
  sm1 = nwsmoothing([vec1])
  sm2 = nwsmoothing([vec2])
  
  pdif = (np.array(sm1) - np.array(sm2))/np.array(sm1)
  
  v = np.var(pdif)
  
  measures = [pen,diff_in_variance,v]
  
  return(nwsmoothing)
  #return(sm2)
  #return(np.sum(np.dot(measures,wts)))


dissimilarity_1(1,2,[0.8])



def cross_dis(mat1, mat2,dissimilarity):
  #' This function calcuates the cross dissimilarity between two groups (row) vectors. it takes in two matrices 
  #' of dimension n x p, m x q and output a n x m dissimilarity matrix
  #' 
  #' ----- Parameters
  #' mat1: Frist group of row vectors
  #' mat2: Second group of row vectors
  #' dissimilarity: the dissimilarity function used in calculation
  #' 
  #' ----- Return:
  #' out: a dissimilarity matrix
  #'
  
  n = mat1.shape[0]
  p = mat2.shape[0]
  
  d = np.zeros((n,p))
  d=pd.DataFrame(d)
  
  for i in range(0,n):
    for j in range(0,p):
      d.iloc[i,j] = dissimilarity(mat1[i,],mat2[j,])
    
  
  
  out = d.copy()
  
  return(out)





def integrity_check(vec,tol):
  #' This function aims at removing observations that has too many missing values
  #' 
  #' ---Parameters
  #' vec = a vector
  #' tol: tolerance of the percentage of missing data
  #' 
  #' ---Return
  #' Returns a vector with missing values repalced with mean if the totla percentage of missing value is less than tolerance
  
  if(sum(vec.isna())/len(vec) <= tol):
    #st.write(sum(vec.isna())/len(vec))
    vec[vec.isna()] = mean([i for i in list(vec) if i==i])
    #st.write(sum(vec.isna())/len(vec))
    #st.write("------------------")
  return(vec)
  
  
def store_pairing(dis):
  
  #counter=0
  n = max(dis.shape)
  p = min(dis.shape)
  
  transposed = False
  if dis.shape[0] != max(dis.shape):
    dis = pd.DataFrame(dis).transpose()
    n = max(dis.shape)
    p = min(dis.shape)
    transposed = True
  
  store_pairs = {'store1': '1', 'store2': '2', 'dissimilarities': '9.23'}
  store_pairs=pd.DataFrame([store_pairs])
  
  # Temporal dissimilarity matrix
  temp_dis = dis
  
 
  while True:
    #counter=counter+1
    #st.write(counter)
    
    s1 = []
    s2 = []
    diff = []
    
    for j in range(0,temp_dis.shape[1]):
      mn=min(temp_dis.iloc[:,j])
      i=np.where(temp_dis.iloc[:,j] == mn)[0][0]
      i=i.item()
      s1 = s1+[i]   
      s2 = s2+[j]
      diff = diff+[mn]
      
    # Construct a prelimilary pairedStores. In this table, stores are likely to be paired multiple times
    store1 = [list(temp_dis.index.values)[s1i] for s1i in s1]
    store2 =[list(temp_dis.columns.values)[s2i] for s2i in s2]
    dissimilarities = diff
    pairedStores = pd.DataFrame()
    pairedStores["store1"]=store1
    pairedStores["store2"]=store2
    pairedStores["dissimilarities"]=dissimilarities
    
    
    pairedStores = pairedStores.sort_values('dissimilarities')
 
    #pairedStores.to_csv("pairedstores_b.csv")
    temp_df=pairedStores[["store1","dissimilarities"]].groupby("store1").min().reset_index()
    pairedStores=temp_df.merge(pairedStores,how="left",on=["store1","dissimilarities"])
    #pairedStores.to_csv("pairedstores_a.csv")  
    
   
    # bind the new store pairs
    store_pairs = store_pairs.append(pairedStores) 
    
    
    #st.write(list(temp_dis.index.values))
    #st.write(list(pairedStores['store1']))
    condition1=list(filter(lambda x: x not in list(pairedStores['store1']), list(temp_dis.index.values)))
    condition2=list(filter(lambda x: x not in list(pairedStores['store2']), list(temp_dis.columns.values)))
   
    #st.write(condition1)
    #st.write(condition2)
    if len(condition1) == 0 | len(condition2) == 0:
        print("here")
        break
    
    temp_dis = pd.DataFrame(temp_dis.iloc[temp_dis.index.isin (condition1),temp_dis.columns.isin (condition2)])
    
    if (store_pairs.shape[0] == min(dis.shape)):
        print("there")
        break
  store_pairs=store_pairs.reset_index()
  store_pairs=store_pairs.drop(0,axis=0)
  #st.write(store_pairs.shape)
  #st.write(store_pairs["dissimilarities"].sum())
  return(store_pairs)

  
    
#store_pairing(dis)    


    
    
    
#numbers = np.arange(20).reshape(5,4) 
#result=list(zip(np.where(numbers==2)[1]))[0][0]
#listOfCoordinates= list(zip(result[1]))[0][0]
#listOfCoordinates    

#store_pairs = pd.DataFrame(columns=['store1','store2','dissimilarities'], index = ['1', '2', '9.23'],dtype=)



