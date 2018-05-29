# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 14:26:04 2018

@author: benmo
"""

import time, datetime, sys, os, numpy as np, pandas as pd, geopandas as gpd
import sqlalchemy
from difflib import SequenceMatcher
import seaborn as sns
import statsmodels.api as sm 
from scipy.stats import rv_discrete
import sqlite3 as db
from itertools import permutations, product, chain
from functools import reduce
import re
import matplotlib.pyplot as plt
import plotly.plotly as py

from pyspark.sql import SparkSession


scriptDir = os.path.dirname(os.path.realpath(sys.argv[0]))   # get script directory

if sys.platform == 'linux':
    genDir ="/home/benmo/OneDrive/GitHub/DataPlayground"
else:
    try: 
        genDir = "D:/OneDrive/GitHub/DataPlayground" #get general functions path
        os.chdir(genDir)
    except: 
        genDir = "D:/benmo/OneDrive/GitHub/DataPlayground"
        os.chdir(genDir)

os.chdir(genDir)

from General import * # import custom 'general' functions

os.chdir(scriptDir)



from IPython import get_ipython
ipython = get_ipython()


ipython.magic("%load_ext rpy2.ipython")

ipython.magic("%R library(SciencesPo)")
ipython.magic("%R library(binsmooth)")


similar = lambda a, b: SequenceMatcher(None, a, b).ratio()


def floatit(x, repl):    
    try:
        x = float(x)
    except:
        x = repl
    return x

get_prob = lambda x: list(map(lambda z, z_s=x: z/sum(z_s)*100,x))

idx = pd.IndexSlice


spark = SparkSession.builder.appName(
        'myspark').master("local[4]").config(
                "spark.ui.enabled","false").config(
                        "spark.driver.extraClassPath",
                        "/home/benmo/DB_drivers/sqlite-jdbc-3.16.1.jar").config(
                                "spark.executor.extraClassPath", 
                                "/home/benmo/DB_drivers/sqlite-jdbc-3.16.1.jar").getOrCreate()



def get_incDist(vals):
    ipython.magic("%R -n bins <- c(10000, 20000, 35000, 50000, 75000, 100000, NaN)")
    ipython.magic("%R -i vals")
    ipython.magic("%R -n hist <- splinebins(bins, vals)")
    bins = ipython.magic("%R seqi <- seq(0,299000,length=10000)")
    cdf = ipython.magic("%R cdf <- hist$splineCDF(seqi)")
    prob = np.diff(cdf)
    bins = bins[1:]
    return rv_discrete(values=(bins, prob/sum(prob)))

def get_gini(data):
    try:
        gini = ipython.magic("%R -i data gini <- Gini(data)")
        return gini[0]
    except:
        return None

def get_atkinson(data, eps=0.5):
    try:
        ipython.magic("%R -i eps")
        atk = ipython.magic("%R -i data atk <- Atkinson(data, epsilon=eps)")
        return atk[0]
    except:
        return None


def closestIndex(x,c):
    temp = list(map(lambda x: similar(x,c), x))
    return temp.index(max(temp))


uniq = lambda x, y: x[y].unique()


tuple2str = lambda name: name if isinstance(name, tuple) ==False else reduce(lambda x, y:  str(x)
        .replace('.0','') + '_' + str(y).replace('.0',''), list(map(lambda xi: str(xi), name)))



def get_Healthx(hRegions, spark, i, j):
    health = spark.read.format("jdbc").option("url", 
                               "jdbc:sqlite:/home/benmo/Data/Databases/health.db").option(
                                       "dbtable", 'CCHS_Survey').load()
    
    healthx = health.select('*').where(((health.AGE == healthVars['age'][i]) & (health.HRPROF == healthVars['hrprof'][j])) & (
                    health.UNIT == healthVars['unit'][1])).toPandas()
                    
    healthx.Geographicalclassification = healthx.Geographicalclassification.apply(
            lambda x: x[:4])
                              
    return healthx



def get_incDistx(provinces, spark, i, j):
    incDist = spark.read.format("jdbc").option("url", 
                               "jdbc:sqlite:/home/benmo/Data/Databases/income.db").option(
                                       "dbtable", "incomeDist").load()

    incDistx = incDist.select(list(filter(lambda x: x not in['Vector', 'Coordinate'],
                                          incDist.columns))).where(incDist.AGE == incDistVars['age'][i]).toPandas() 
    incDistx = incDistx[incDistx.GEO.apply(lambda x: x not in provinces + ['Canada']) == True]
    incDistx.GEO = incDistx.GEO.apply(lambda x: x[:12] if x.find(',') >=12 else x[:x.find(',')])
    
    return incDistx 
    
    
uri="postgresql://postgres:justbusted@localhost:5433/geodata"
engine = sqlalchemy.create_engine(uri)


stata = 0    # dont write to stata files in section 4.1
shape = 1    # write the shapefiles in section 4.1

healthShp = gpd.read_file("/home/benmo/Data/GeoData/canHealth.shp")               # get shapefile for health regions
canadaShp = gpd.read_postgis("select * from can_adm3", uri, 
                        geom_col='geom', crs={'init': 'epsg:4326'})               # get shapefile for Canada (incl. city names)


    
healthVars, incDistVars = {}, {}

healthVars['hrprof'] = ['Current smoker, daily or occasional', 
'Arthritis', 'Current smoker, daily',  
'5 or more drinks on one occasion, at least once a month in the past year', 'Heavy drinking', 
'Fruit and vegetable consumption, 5 times or more per day', 
'Physical activity during leisure-time, moderately active or active', 
'Physical activity during leisure-time, inactive', 
'Body mass index, self-reported, adult (18 years and over), overweight or obese',  
'Body mass index, self-reported, adult (18 years and over), obese', 
'Sense of belonging to local community, somewhat strong or very strong', 
'Has a regular medical doctor', 
'Contact with a medical doctor in the past 12 months',  
'Mood disorder', 'Wears a helmet when riding a bicycle, always', 
'Functional health, good to full', 
'Injuries in the past 12 months causing limitation of normal activities', 'Injuries in the past 12 months, sought medical attention', 
'Chronic obstructive pulmonary disease (COPD)', 
'Perceived health, very good or excellent', 
'Perceived health, fair or poor', 
'Perceived mental health, very good or excellent', 
'Perceived mental health, fair or poor', 
'Life satisfaction, satisfied or very satisfied', 
'Perceived life stress, quite a lot (15 years and over)', 
'Diabetes', 'Asthma', 'High blood pressure', 
'Pain or discomfort by severity, moderate or severe', 
'Pain or discomfort that prevents activities', 
'Participation and activity limitation, sometimes or often']


healthVars['unit'] = ['Number of persons', 'Percent']

healthVars['age'] = ['20 to 34 years', '35 to 44 years',
        '45 to 64 years', 'Total, 12 years and over']

incDistVars['age'] = ['25 to 34 years', '35 to 44 years', 
        '45 to 64 years', 'All age groups']

canadaShp['geo_idx'] = canadaShp.index.tolist()         #create column containing unique city index


cities_inH = gpd.sjoin(canadaShp, healthShp, 
                       how='left', op= 'within')        #spatially join city & health region shapefiles

#note: this operation only joins cities w/ boundaries completely within health regions

cities_inH = cities_inH[cities_inH['HR_UID'].isna() == False] #drop cities not within h-regions


missingcities = gpd.sjoin(canadaShp, healthShp, 
                          how='left', op= 'within')           #collect cities missed by spatial join

missingcities = missingcities[missingcities['HR_UID'].isna() == True][
        missingcities['engtype_3'] == 'City']                 #select only cities (pop > ~10,000)

missingcities.geom = missingcities.geom.apply(lambda x: x.centroid) #get centroid of cities

missingcities = gpd.sjoin(missingcities[list(filter(lambda x: 
                                                    x not in ['index_left','index_right'],
                                                    missingcities.columns.tolist()))],
                                        healthShp, how='left', op='within') #add the missing cities 

#note: missing cities added to shapefile by joining city centroid to the appropriate h-region

cities = cities_inH.append(missingcities)       #collect all the cities (missing/non-missing) to one shapefile


"""_____________Merge the Health Shapefile with the edited Canada-City shapefile___________"""

healthCanShp = gpd.sjoin(healthShp, cities[['geom', 'geo_idx', 'name_1', 
                                               'name_3', 'engtype_3'] ], how='left', op='contains')

    
healthCanShp.name_3[healthCanShp.geo_idx == 3367] = 'Montréal' # Montreal suburb (location same as Montreal)
healthCanShp.geo_idx[healthCanShp.geo_idx == 3367] = 3368      # Actual Montreal id

  
provinces = canadaShp['name_1'].unique().tolist()
hRegions = healthShp.HR_UID.unique().tolist()


def get_data(healthVars, incDistVars, spark, stata=0, shape=0):
    
    for i, (varH, varI) in enumerate(zip(healthVars['age'], incDistVars['age'])):

        for j, hType in enumerate(healthVars['hrprof']):  #start ay number 5!!!!!!!
    
            """_____________HEALTH DATA_______________"""
    
            healthx = get_Healthx(hRegions,spark, i, j) # get CCHS data for specific health variable (j) & age (i)
    
            healthData = healthShp.merge(healthx.drop(columns=['Vector','Coordinate']), how ='left', left_on='HR_UID', 
                                                                 right_on='Geographicalclassification') #merge w/ geographical data
            healthData.rename(columns={'Ref_Date' : 'ref_date', 'GEO' : 'geo', 'AGE' : 'age', 'SEX' : 'sex',
                                      'HRPROF' : 'hrprof', 'Geographicalclassification' : 'geographicalclassification',
                                      'Value' : 'value'}, inplace=True)
            healthData.value = pd.to_numeric(healthData.value,errors='coerce')
        
            
            #===================> only need to calculate income distributions for each age range (ie. j = 0) ===========>
            if j==0:
    
                """_____________INCOME DATA_______________"""
    
                incDistx = get_incDistx(provinces,spark, i,j) # get age/sex specific income distribution data
                cities = incDistx.GEO.unique()                # get list of cities incl. in income data
                
                cities = pd.DataFrame(data=np.array([cities, list(map(lambda x: closestIndex(canadaShp['name_3'], x), cities))]).T,
                            columns=['geo','geo_id'])         # get geo_id of cities in income data
    
                
                incDistx.rename(columns={'GEO' : 'geo'}, inplace=True)
                incDistx = incDistx.merge(cities, how='left', on='geo') # data now includes geo_id for merging
    
    
    
                """_____________FORMAT DATA_______________"""
    
                temp = incDistx.rename(columns={'Ref_Date' : 'ref_date', 'GEO' : 'geo', 'SEX' : 'sex', 'AGE' : 'age', 'INC' : 'inc', 'Value' : 'value'})
                
                
                #================> the following code re-shapes the data and creates a column for each income bin =====>
                incTable = pd.pivot_table(temp[['ref_date', 'geo','geo_id', 'sex','inc','value']], 
                                    values='value', index=['ref_date', 'geo','geo_id', 'sex'],columns=['inc'],aggfunc=np.mean)
                
                try:
                    incTable.drop(columns=['5-year percent change of median income',
                                           'Median total income (dollars)'],inplace=True) # delete unused cols if they exist
                except:
                    pass
                
                renamecol = lambda x: re.sub('[^0-9]','', x) # define func. to rename income bins to the upper bin range value
                
                incTable = incTable.rename(columns=dict(zip(incTable.columns[:-1].tolist(), 
                                                            list(map(renamecol, incTable.columns[:-1].tolist()))))) # rename cols
    
                #===============> The following code converts data from total persons making over $x to a % of population====>
                incTable['200000'] = incTable.apply(lambda x: x['100000']/x[incTable.columns[-1]]*100, axis=1)
                incTable['100000'] = incTable.apply(lambda x: (x['75000'] - x['100000'])/x[incTable.columns[-1]]*100, axis=1) 
                incTable['75000'] = incTable.apply(lambda x: (x['50000'] - x['75000'])/x[incTable.columns[-1]]*100, axis=1)
                incTable['50000'] = incTable.apply(lambda x: (x['35000'] - x['50000'])/x[incTable.columns[-1]]*100, axis=1)  
                incTable['35000'] = incTable.apply(lambda x: (x['20000'] - x['35000'])/x[incTable.columns[-1]]*100, axis=1) 
                incTable['20000'] = incTable.apply(lambda x: (x['10000'] - x['20000'])/x[incTable.columns[-1]]*100, axis=1) 
                incTable['10000'] = incTable.apply(lambda x: (x[incTable.columns[-1]] - x['10000'])/x[incTable.columns[-1]]*100, axis=1)
    
    
    
                incTable.rename(columns={'200000' : 'Over 100000'}, inplace=True)                # rename cols
                incTable.drop(columns=['5000','15000','25000','150000', '250000'],inplace=True)  # drop unused cols
    
                
                for col in incTable.columns.tolist():
                    incTable[col][incTable[col].isna() == True] = 0     # na values have 0% of population within bin
                    
                incTable['totalpercent'] = incTable.apply(
                        lambda x: sum([float(x[xi]) for xi in incTable.columns[:-1].tolist()]), axis=1) # used to check if %'s add up to 1
    
                #=============> send data to R and return smoothed income distribution ==========> 
                incTable['dist'] = incTable.apply(lambda x: get_incDist([x['10000'],
                                                                         x['20000'], x['35000'],
                                                                         x['50000'], x['75000'],
                                                                         x['100000'], x['Over 100000']]), axis=1)
    
                #===========> creates data for more refined income bins generated by the smoothed distribution ===>
                #===============> calls R to calculate inequality metrics given the income distributions =========>
                incTable['tempdist'] = incTable.dist.apply(lambda x: x.rvs(size=30000))
                incTable['gini'] = incTable.apply(lambda x: get_gini(x['tempdist']), axis=1)
                incTable['median'] = incTable.apply(lambda x: x['dist'].median(), axis=1)
                incTable['atkLow'] = incTable.apply(lambda x: get_atkinson(x['tempdist'], eps=0.5), axis=1)
                incTable['atkHigh'] = incTable.apply(lambda x: get_atkinson(x['tempdist'], eps=1.5), axis=1)
                incTable = incTable.drop(columns='tempdist')
    
                #============> the only variables which are needed later are retained in the 'temp' dataframe ======>
                temp = incTable[['Total persons with income','gini', 'atkLow',
                                 'atkHigh','median']].apply(
                                 pd.to_numeric).set_index(
                                         incTable.index.get_level_values(2).astype(int))
                
                
                
                #===========> reshape dataframe (ie. move sex,year from index to columns) ===>
                temp.index.set_names('x', inplace=True)
                temp['Year'] = incTable.index.get_level_values(0).astype(int).tolist()
                temp['geo_id'] = incTable.index.get_level_values(2).tolist()
                temp['Sex'] = incTable.index.get_level_values(3).astype(str).tolist()
    
                tempInc = pd.pivot_table(temp[['Year','geo_id', 'Sex', 
                                                   'Total persons with income', 'gini',
                                                   'atkLow','atkHigh','median']], 
                                                    values=['Total persons with income', 'gini',
                                                            'atkLow','atkHigh', 'median'], index='geo_id',
                                                            columns=['Year', 'Sex'],aggfunc=np.mean)
    
                # note: this makes it so that the only index is geography, such that all the data can easily be joined
                # on the geo_id and saved as a shapefile for geographical visualization/processing.
    
            
            #====================> Reshape health data for merging with income data =============>
            healthData.rename(columns={'ref_date' : 'Year', 'age' : 'Age', 'sex' : 'Sex'}, inplace = True)
    
            healthData[['Age','Sex', 'hrprof', 'unit']] = healthData[['Age',
                   'Sex', 'hrprof', 'unit']].astype(str)
    
            healthData[['Year','value']] = healthData[['Year','value']].apply(pd.to_numeric)
    
            tempHealth = pd.pivot_table(healthData[['HR_UID',
                                        'Year', 'Sex', 'value']], 
                                                values=['value'], index=['HR_UID'],
                                                        columns=['Year', 'Sex'],aggfunc=np.mean)
    
    
            """_______SHAPEFILES!!!!!!!!!!_________"""
            ineqShapeCities = healthCanShp.merge(tempInc, how='inner', left_on='geo_idx', right_index=True)
            healthShapeRegions = healthCanShp.merge(tempHealth, left_on='HR_UID', 
                                                  right_index=True).merge(
                                                          tempInc, left_on='geo_idx', right_index=True)
    
    
    
    
            """________CREATE DATA TABLE____________"""
    
            regionsData = healthCanShp.merge(temp, how='inner', left_on='geo_idx', right_index=True)
            regionsData.rename(columns={'hr_uid' : 'HR_UID', 'eng_label' : 'ENG_LABEL'}, inplace=True)
            regionsData[['HR_UID', 'ENG_LABEL', 'name_3', 'geo_idx']] = regionsData[['HR_UID', 'ENG_LABEL', 'name_3', 'geo_idx']].astype(str)
            healthData.rename(columns={'ref_date' : 'Year', 'age' : 'Age', 'sex' : 'Sex'}, inplace = True)
    
            healthData[['Age','Sex', 'hrprof', 'unit']] = healthData[['Age',
                   'Sex', 'hrprof', 'unit']].astype(str)
            healthData[['Year','value']] = healthData[['Year','value']].apply(pd.to_numeric)
    
            regionsData = regionsData.merge(healthData[['HR_UID','Year', 'Age',
                   'Sex', 'hrprof', 'unit', 'value']], how='left', on=['HR_UID','Year','Sex'])
    
    
    
            diri = "/home/benmo/Data/GeoData/Inequality/{age}".format(age=varI)
            dirj = "/{htype}".format(htype=hType)
    
            if not os.path.exists(diri[:35] + dirj):
                os.makedirs(diri[:35] + dirj)
    
            """______________________SEND TO STATA _____________________________"""
            if stata == 1:
                regionsData[varI] = varI
                regionsData[hType] = hType
                regionsData[['HR_UID', 'ENG_LABEL', 'name_1', 'name_3', 'geo_idx', 'gini','atkLow','atkHigh','median','Year','Sex', 'Age', 'hrprof',
                             'Total persons with income', 'unit', 'value'] + [
                                     varI, hType]].astype(str).to_stata(diri[:35] + dirj + "/stataHealth.dta")
    
    
            """______________________SEND TO PICKLE _____________________________"""
    
            regionsData[varI] = varI
            regionsData[hType] = hType
            regionsData[['HR_UID', 'ENG_LABEL', 'name_1', 'name_3', 'geo_idx', 'gini','atkLow','atkHigh','median','Year','Sex', 'Age', 'hrprof',
                         'Total persons with income', 'unit', 'value'] + [
                                 varI, hType]].to_pickle(diri[:35] + dirj + "/pyHealth.pkl")
            if j == 0:
                incTable.to_pickle(diri[:35] + dirj + "/pyIncomeTable.pkl")
    
            ineqShapeCities.to_pickle(diri[:35] + dirj + "/pyShpCities.pkl") 
            healthShapeRegions.to_pickle(diri[:35] + dirj + "/pyShpRegions.pkl")
    
    
            """______________________SAVE SHAPEFILES TO DISK_____________________________"""
    
            if shape == 1:
                ineqShapeCities.rename(columns=lambda x: tuple2str(x), inplace=True)
                healthShapeRegions.rename(columns=lambda x: tuple2str(x), inplace=True)
    
                ineqShapeCities.to_file(diri[:35] + dirj + "/ineqCity.shp", driver='ESRI Shapefile')
                healthShapeRegions.to_file(diri[:35] + dirj + "/ineqRegion.shp", driver='ESRI Shapefile')

    spark.stop()

sex=['Both sexes', 'Males', 'Females']
    
def plotDist(data, sex):
    dists2000 = [data.loc[idx['2003',city, :, sex]].dist for city in ['Québec', 'Montréal',
             'Ottawa-Gatin', 'Calgary', 'Edmonton', 'Toronto', 'Winnipeg', 'Vancouver']]
    dists2015 = [data.loc[idx['2015',city, :, sex]].dist for city in ['Québec', 'Montréal',
             'Ottawa-Gatin', 'Calgary', 'Edmonton', 'Toronto', 'Winnipeg', 'Vancouver']]
    ax = {}
    fig = plt.figure()
    ax[0] = fig.add_subplot(421)
    ax[1] = fig.add_subplot(422)
    ax[2] = fig.add_subplot(423)
    ax[3] = fig.add_subplot(424)
    ax[4] = fig.add_subplot(425)
    ax[5] = fig.add_subplot(426)
    ax[6] = fig.add_subplot(427)
    ax[7] = fig.add_subplot(428)
    
    for i, (dist2k, dist2k15) in enumerate(zip(dists2000, dists2015)):
        sns.distplot(dist2k[0].rvs(size=20000), hist=False, label=dist2k.index.values[0][1],
                     ax=ax[i])
        sns.distplot(dist2k15[0].rvs(size=20000), hist=False, label=dist2k15.index.values[0][1],
                     ax=ax[i])
           

def get_IncTable():
    ages = ['25 to 34 years','35 to 44 years','55 to 64 years','All age groups']
    table = pd.DataFrame([])
    for age in ages:
        temp = pd.read_pickle("D:/Data/Econ/Health/{a}/pyincometable.pkl".format(
                a=age))
        temp.drop(columns='dist', inplace=True)
        temp['Year'] = temp.index.get_level_values(0)
        temp['City'] = temp.index.get_level_values(1)
        temp['Sex'] = temp.index.get_level_values(3)
        temp['Age'] = age
        temp.reset_index(inplace=True)
        table = table.append(temp)
    return table

def dataAvail(healthVars):
    data = pd.read_stata("Z:/Health/healthstata.dta")
    regionyears = list(product(data.HR_UID.unique(), data.Year.unique()))
    sexList = ['Both sexes', 'Males', 'Females']
    cats = data.columns[20:].tolist()
    
    availDict = {}
    for sex in sexList:
        availDict[sex] = {}
        for age in healthVars['age']:
            availDict[sex][age] = []
            for i, cat in enumerate(cats):
                temp = data[(data.Age == age) & (data.Sex == sex)]
                temp = temp[data[cat].isna() == False]
                pct = len(list(product(temp.HR_UID.unique(), temp.Year.unique())))/len(regionyears)*100
                if i == 0:
                    availDict[sex][age] = [cat, pct]
                else:   
                    availDict[sex][age] = np.vstack([availDict[sex][age], [cat, pct]])
        
    return availDict







"""spark = SparkSession.builder.appName(
        'myspark2').master("local[4]").config(
                "spark.ui.enabled","false").config(
                        "spark.driver.extraClassPath",
                        "D:/DBdrivers/sqlite-jdbc-3.16.1.jar").config(
                                "spark.executor.extraClassPath", 
                                "D:/DBdrivers/sqlite-jdbc-3.16.1.jar").getOrCreate()

bob = spark.read.format("jdbc").option("url", 
                               "jdbc:sqlite:D:/Data/Econ/Income/Canada/Canada.db").option(
                                       "dbtable", "incomeDist").load()"""

"""

ageList = ['25 to 34 years','35 to 44 years',
 '55 to 64 years','All age groups']

catList = list(filter(lambda x: x in ['Current smoker, daily or occasional', 
'Arthritis', 'Current smoker, daily',  
'5 or more drinks on one occasion, at least once a month in the past year', 'Heavy drinking', 
'Fruit and vegetable consumption, 5 times or more per day', 
'Physical activity during leisure-time, moderately active or active', 
'Physical activity during leisure-time, inactive', 
'Body mass index, self-reported, adult (18 years and over), overweight or obese',  
'Body mass index, self-reported, adult (18 years and over), obese', 
'Sense of belonging to local community, somewhat strong or very strong', 
'Has a regular medical doctor', 
'Contact with a medical doctor in the past 12 months',  
'Mood disorder', 'Wears a helmet when riding a bicycle, always', 
'Functional health, good to full', 
'Injuries in the past 12 months causing limitation of normal activities', 'Injuries in the past 12 months, sought medical attention', 
'Chronic obstructive pulmonary disease (COPD)', 
'Perceived health, very good or excellent', 
'Perceived health, fair or poor', 
'Perceived mental health, very good or excellent', 
'Perceived mental health, fair or poor', 
'Life satisfaction, satisfied or very satisfied', 
'Perceived life stress, quite a lot (15 years and over)', 
'Diabetes', 'Asthma', 'High blood pressure', 
'Pain or discomfort by severity, moderate or severe', 
'Pain or discomfort that prevents activities', 
'Participation and activity limitation, sometimes or often'], os.listdir("D:/Data/Econ/Health/All age groups")))


columns = ['HR_UID', 'ENG_LABEL', 'name_1', 'name_3', 'geo_idx', 'gini','atkLow','atkHigh', 'median',
       'Year', 'Sex', 'Age', 'hrprof', 'Total persons with income', 'unit',
       'value']

data = pd.DataFrame([])

for age in ageList:
    dfListj = [pd.read_pickle("D:/Data/Econ/Health/{i}/{j}/pyHealth.pkl".format(i=age, j=cati)) for cati in catList]    
    temp = pd.DataFrame(reduce(lambda x, y: x[columns].append(y[columns], ignore_index=True),dfListj))
    data = data.append(temp, ignore_index=True)


food = spark.read.format("jdbc").option("url", 
                               "jdbc:sqlite:D:/Data/Econ/Health/health.db").option(
                                       "dbtable", "FoodInsec").load()

food_df = food.select('*').where((food.STATUS == 'Food insecure, moderate and severe') & 
                    (food.Ref_Date == '2011/2012') & (
                    food.UNIT == 'Percent of persons in households')).toPandas()


food_df.Geographicalclassification = food_df.Geographicalclassification.apply(
            lambda x: x[:4])

food_df.rename(columns={'Ref_Date' : 'Year','Geographicalclassification' : 'HR_UID','AGE' : 'Age',
 'SEX' : 'Sex', 'ValueFood' : 'Pct Food Insecure'}, inplace=True)
    
    
food_df.GEO[food_df.GEO == 'Quebec'] = 'Québec'

food_df['Prov Food Insecurity'] = food_df['Pct Food Insecure']

data = data.merge(food_df[['GEO', 'Age', 'Sex', 'Prov Food Insecurity']], 
           how='left', left_on=['name_1', 'Age', 'Sex'],
           right_on=['GEO', 'Age', 'Sex'])

data = data.merge(food_df[['HR_UID', 'Age', 'Sex', 'Pct Food Insecure']], 
           how='left', on=['HR_UID', 'Age', 'Sex'])

data = data[data.Age.isna() == False]

data[['HR_UID','geo_idx', 'gini','atkLow','atkHigh', 'median',
       'Year', 'Total persons with income',
       'value', 'Prov Food Insecurity', 'Pct Food Insecure']] = data[
    ['HR_UID','geo_idx', 'gini','atkLow','atkHigh', 'median',
       'Year', 'Total persons with income',
       'value', 'Prov Food Insecurity', 'Pct Food Insecure']].apply(pd.to_numeric, errors = 'coerce')

data[['ENG_LABEL', 'name_1', 'name_3',
       'Sex', 'Age', 'hrprof', 'unit', 'GEO']] = data[
    ['ENG_LABEL', 'name_1', 'name_3',
       'Sex', 'Age', 'hrprof', 'unit', 'GEO']].astype(str)
    


temp = pd.pivot_table(data[['HR_UID', 'geo_idx', 'Year', 'Sex', 'Age','hrprof', 'value']], 
                     values = ['value'], index = ['HR_UID', 'geo_idx', 'Year', 'Sex', 'Age'],
                     columns = ['hrprof'])

for i in range(len(temp.index.names)):
    temp[temp.index.names[i]] = temp.index.get_level_values(i)

data = data.merge(temp, how='outer', left_on = ['HR_UID', 'geo_idx', 'Year', 'Sex', 'Age'], right_on=temp.columns.levels[0][1:].tolist())

data.rename(columns = lambda x: tuple2str(x), inplace=True)

data.to_stata("D:/Data/Econ/Health/healthStata.dta")

"""