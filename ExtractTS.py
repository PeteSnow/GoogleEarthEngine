# -*- coding: utf-8 -*-
"""
Created on Sat May 23 11:56:08 2020

@author: sosnowski
"""

# %%

# Import the required packages
import timeit
import time
import math
import datetime as dt
import ee
from geextract import ts_extract, get_date
from pandas.core.common import flatten
import pickle
import pandas as pd
import numpy as np

ee.Authenticate()
ee.Initialize() # Initialize Google Earth Engine Session

# %%

# define the parameters of the time-serie extraction function
product = 'UCSB-CHG/CHIRPS/DAILY'
bands = ['precipitation']
band_name = ['precipitation','time']
prod_id = 'chirps'
locations = ee.FeatureCollection('users/pedrososno/RainGauges')
year1 = 2013
year2 = 2019
timeformat =  '%Y%m%d'
savepath = '/Users/sosnowski/Dropbox/Tropical_Environment_Conservation/TS_Analysis/'

# Define the dictionary in which to save iteratively the computations.
# In this case, we create a new dictionary.

Githubtest = {}
with open(savepath + savingdic + '.pickle', 'wb') as handle:
    pickle.dump(Githubtest, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()
    
pickle.load(open(savepath + 'Githubtest.pickle','rb'))

savingdic = 'Githubtest'


# %%

# Subfunctions necessary for the main time-serie extraction function

def addfeat(ft):
    newft = ft.set({'location':ft.id()})
    return newft

def getft(img):
    newim = img.addBands(ee.Number.parse(img.get('system:index'))).rename(band_name);
    ## create a feature from this newimg storing the two bands as properties.
    ## Do it or all points. This will give a featurecollection with 6 features for the 6 spoints
    ft = newim.reduceRegions(locations, ee.Reducer.first());
    ## add a property to the features of the featurecollection: the id of station
    ft2 = ft.map(addfeat)
    return ft2


# The main time-serie extraction function
def ExtractTS(product,bands,band_name,prod_id,locations,year1,year2,timeformat,savepath,savingdic):
    
    column_name = [band_name,  'location']
    column_name = list(flatten(column_name))
    
    def Subdf(dic):
        subdf = {}
        for colnam in column_name: 
            if colnam in dic.keys():
               
                subdf[colnam] = dic[colnam]
            else:
                subdf[colnam] = np.nan
        
        subdf['time'] = pd.to_datetime(subdf['time'],format = timeformat)
     
        return subdf
 
    Savedic = pickle.load(open(savepath + savingdic + '.pickle','rb'))
    TOTALPROCESS1 = time.time()
    
    for i in range(year1,year2+1):
        
        START = str(i) + '-01-01'
        END = str(i) +'-12-31'
        imgcoll = ee.ImageCollection(product).filterDate(START, END)
    
        
        Range = imgcoll.reduceColumns(ee.Reducer.minMax(), ["system:time_start"])
        size = imgcoll.size()
        size2 = locations.size()
        tstep = (Range.getNumber('max').subtract(Range.getNumber('min'))).divide(size.multiply(size2)) # can only extract 'size2' less dates because of multiple points
        
        
        ################ CREATE A LOOP TO ONLY GET INFO PER PACK OF 4500 ELEMENTS (TO AVOID CRASH) ##############
        # Initial time limits 
        TF = pd.to_datetime(START)
        QUIT =  pd.to_datetime(END)
        interv = np.timedelta64(60*15, 's')
    
                # (minus a timestep because imgcoll never collects the last data)
                # Because of that, the loop TF < END would run indefinetly. 
                # use an approximate equalization between TF and the QUIT date assignement. set initial precision to 15 min
        
        subdfmapdic = {}
        startbigloop = time.time()
        i = -1
        FINALDF = pd.DataFrame(columns = column_name)  
        
        while (QUIT - TF) > interv:
            i = i + 1
            ti = ee.Date(TF).millis()
            tf = ti.add(tstep.multiply(4500))
            
            startdate = ee.Date(ti)
            enddate = ee.Date(tf)
        
            imgcoll_r = imgcoll\
                    .filterDate(startdate, enddate)\
                    .select(bands)
            
            Output = imgcoll_r.map(getft)
            
            flt = Output.flatten()
            
            ##################### CLIENT-SIDE FUNCTION : EXTRACTING THE FEATURES INTO A PYTHON READABLE FORMAT ###########
            # time the first loop
            start = time.time()
            out = flt.getInfo()['features']
            end = time.time()
            print('Time to extract the features:',end-start)
            
            ##################### FORMAT THE OUTPUT INTO A DATAFRAME ################################
            # map a function to all dictionaries of a list
            
            # the function is to get just the properties of all elements
            start = time.time()
            propmap = map(lambda x: x['properties'],out)
            
    
            # get subfs with map for all dictionaries in Res
            subdfmap = map(Subdf,list(propmap))         
            end = time.time()
            #print('Time with map to map properties and get subdf in a map', end-start)
            
            #store all the subdfmap functions into a dic for the final step after all 4500*x elements
            listsubdf = list(subdfmap)
            subdfmapdic[i] = listsubdf
            
            #################### SET THE NEW TIME LIMITS #########################
            # Get the last two unique dates
            dates = pd.Series(map(lambda x: x['time'], listsubdf))
            unique_t = dates.unique()
            
            # Get last date
            TF = pd.to_datetime(unique_t[-1])
            
            # Get date before last date
            TF0 = pd.to_datetime(unique_t[-2])
            
            # Compute interval
            interv = TF - TF0
            
            # set new quit condition
            QUIT =  pd.to_datetime(END) - interv
            
        
        
        # append all subdfs of 4500 elements dictionarites into one big DF for the whole year
        start = time.time()
        
        for y in subdfmapdic:
            DF = pd.DataFrame(subdfmapdic[y]) # DataFrame is faster than .from_dict and from_records
            FINALDF = FINALDF.append(DF)
        
        end = time.time()
        print('Time to convert list of dic into a big dataframe', end-start)
        # set index of DF
        FINALDF.index = FINALDF['time']
        FINALDF = FINALDF.drop('time',1)    
        
        endbigloop = time.time()
        print('Time to get the whole year into one dataframe including extraction process',
              endbigloop - startbigloop)
        
        ################### SAVE THE FINAL BIG DF INTO A DIC WITH A KEY FOR THE YEAR #############
        
        Savedic[prod_id,START] = FINALDF
    
        with open(savepath + savingdic + '.pickle', 'wb') as handle:
            pickle.dump(Savedic, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.close()
    
        Savedic = pickle.load(open(savepath + savingdic +'.pickle','rb'))
        
    TOTALPROCESS2 = time.time()
           
    print('Time for the whole process for all years', TOTALPROCESS2 - TOTALPROCESS1) 
    return Savedic

# %%
    
# Try out the function
Githubtest = ExtractTS(product,bands,band_name,prod_id,locations,year1,year2,timeformat,savepath,savingdic)
Githubtest = pickle.load(open(savepath + 'Githubtest.pickle','rb'))
    

