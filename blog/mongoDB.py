#
##-*- coding:utf-8 -*-

from pymongo import MongoClient
from datetime import datetime

import pandas as pd
import json

class MongoConnector():
    def __init__(self, dbName, collectionName):
        self.server = 'localhost'
        self.username = 'sujin'
        self.password = 'sujin'
        self.dbName = dbName
        self.collectionName = collectionName
        self.Client = None
        self.db = None
        self.collection = None
        self.keyField = None

    def connect(self):
        try:
            self.Client = MongoClient(self.server,
                                      username = self.username,
                                      password = self.password,
                                      authSource = 'admin',
                                      authMechanism='SCRAM-SHA-256')
            self.db = self.Client.get_database(self.dbName)
            self.collection = self.db.get_collection(self.collectionName)
            print('[ - ] DB connected! '+self.dbName+' / '+self.collectionName)
        except Exception as e:
            print('[ ! ] DB connection failed! Check MongoConnector inputs')
            print(e)

    def setKeyField(self, keyField):
        self.keyField = keyField

    def isDuplicate(self, data):
        if self.collection.count_documents({self.keyField:data[self.keyField]}) != 0:
            return True
        else:
            return False
    
    def insertOne(self, data):
        try:
            if self.keyField == None:
                print("[ ! ] Set key field first! -> db.setField('keyFieldName')")
                raise Exception
            
            if not self.isDuplicate(data):
                self.collection.insert_one(data)
                print('[ - ] Inserted!')
            else:
                print('[ - ] Duplicate '+self.keyField+'!')

        except Exception as e:
            print('[ ! ] insertOne error')
            print(e)
    
    def insertMany(self, data):
        try:
            if self.keyField == None:
                print("[ ! ] Set key field first! -> db.setField('keyFieldName')")
                raise Exception
                        
            for each in data:
                if not self.isDuplicate(each):
                    self.collection.insert_one(each)
                    print('[ - ] Inserted! '+each[self.keyField])
                else:
                    print('[ - ] Duplicate '+self.keyField+'! '+each[self.keyField])

        except Exception as e:
            print('[ ! ] insert error')
            print(e)

    def updateOne(self, targetField,targetCont,updateField,updateCont):
        try:
            self.collection.update_one({targetField:targetCont}, 
                                       {'$set':{updateField:updateCont}})
            print('[ - ] Updated!')
        except Exception as e:
            print('[ ! ] updateOne error')
            print(e)

    def updateMany(self, targetField, targetCont, updateDict):
        try:
            self.collection.update({targetField:targetCont}, 
                                   {'$set':updateDict})

            print('[ - ] Updated!')
        except Exception as e:
            print('[ ! ] updateMany error')
            print(e)
        
    def len(self):
        try:
            print('[ - ] '+str(self.collection.count_documents({})))
        except Exception as e:
            print(e)

    def deleteOne(self, targetField=None, targetCont=None):
        try:
            if targetField != None and targetCont != None:
                count = self.collection.count_documents({targetField:targetCont})
                print('[ - ] Find '+str(count)+' documents')
                self.collection.delete_one({targetField:targetCont})
                print('[ - ] Delete one!')
            else:
                print('[ ! ] Set targetField and targetCont first!')
                raise Exception
        except Exception as e:
            print('[ ! ] deleteOne error')
            print(e)

    def deleteAll(self):
        try:
            self.collection.delete_many({})
            print('[ - ] Delete all!')
        except Exception as e:
            print('[ ! ] deleteAll error')
            print(e)
    
    def find(self, targetField=None, targetCont=None):
        try:
            if targetField == None:
                count = self.collection.count_documents({})
                print('[ - ] Find '+str(count)+' documents')
                return self.collection.find({})
            else:
                count = self.collection.count_documents({targetField:targetCont})
                print('[ - ] Find '+str(count)+' documents')
                return self.collection.find({targetField:targetCont})
        except Exception as e:
            print('[ ! ] find error')
            print(e)
            return None
        
    def findStringContains(self, targetField=None, targetCont=None):
        try:
            if targetField == None or targetCont == None:
                raise Exception
            targetString = '.*'+targetCont+'.*'
            count = self.collection.count_documents({targetField : {"$regex" : targetString}})
            print('[ - ] Find '+str(count)+' documents')
            return self.collection.find({targetField : {"$regex" : targetString}})
        except Exception as e:
            print('[ ! ] find String Contains error')
            print(e)
            #return None
    
    def findAnd(self, targetList):
        try:
            pass
        except:
            pass

    def findOr(self, targetList):
        try:
            pass
        except:
            pass

    def makeCSV(self, targetField=None, targetCont=None, columnList=None, saveLocation='', encoding='utf-8'):
        try:
            if (targetField == None and targetCont != None) or (targetField != None and targetCont == None):
                print("[ ! ] Set 'targetField' and 'targetCont' ")
                raise Exception

            fieldNames = [str(each) for each in self.collection.find_one().keys()]

            if '_id' in fieldNames:
                fieldNames.remove('_id')

            # 컬럼을 지정하지 않으면 모든 필드 가져옴.
            if columnList == None:
                columnList = fieldNames

            # csv 만들 데이터 담을 공간 지정함.
            data = []
            for x in range(len(columnList)):
                col = []
                data.append(col)

            # 특정 항목을 지정하지 않으면 전체 항목 데이터를 가져옴.
            if targetField == None:
                for item in self.find():
                    for colIndex in range(len(data)):
                        field = columnList[colIndex]   # columnList와 data sync 맞춰줌.
                        data[colIndex].append(str(item[field]))
            # 특정 항목을 지정하면 해당 항목 데이터만 가져옴. ex) lang 필드 값이 python인것만 가져오기
            else:
                for item in self.find(targetField,targetCont):
                    for colIndex in range(len(data)):
                        field = columnList[colIndex]   # columnList와 data sync 맞춰줌.
                        data[colIndex].append(str(item[field]))                    

            df = pd.DataFrame()

            for i, colNames in enumerate(columnList):
                df[colNames] = data[i]

            df = df.sort_values(by=self.keyField) 
            
            fileName = self.collectionName+'_'+datetime.today().strftime("%Y%m%d_%H%M") +'.csv'
            
            df.to_csv(saveLocation+fileName, encoding=encoding, index=False)

            print('[ - ] CSV saved at '+ saveLocation+fileName)
            
            return saveLocation+fileName, df

        except Exception as e:
            print('[ ! ] makeCSV error')
            print(e)
    
    def makeJSON(self, targetField=None, targetCont=None, columnList=None, dateColName=None, saveLocation='', encoding='utf-8'):
        if (targetField == None and targetCont != None) or (targetField != None and targetCont == None):
            print("[ ! ] Set 'targetField' and 'targetCont' ")     
            
        fieldNames = [str(each) for each in self.collection.find_one().keys()]   
        
        # 컬럼을 지정하지 않으면 모든 필드 가져옴.
        if columnList == None:
            columnList = fieldNames        
        
        if targetField == None:
            ea = self.find()
        else:
            ea = self.find(targetField, targetCont)
            
        res = {'nData':0,
                'data':[]
               }
        for each in ea:
            data = {}
            for key in list(each.keys()):
                value = each[key]
                if key == '_id' or key not in columnList:
                    continue
                elif key == dateColName and type(each[key]) == datetime:
                    year = str(each[key].year)
                    month = str(each[key].month)
                    day = str(each[key].day)
                    hour = str(each[key].hour)
                    minute = str(each[key].minute)
                    second = str(each[key].second)

                    value = f'{year}-{month}-{day} {hour}:{minute}:{second}'
                data[key] = value
            res['data'].append(data)
            #print(data)
        res['nData'] = len(res['data'])
        fileName = self.dbName+'_'+self.collectionName+'_'+datetime.today().strftime("%Y%m%d_%H%M")+'.json'

        with open(saveLocation+fileName, 'w', encoding=encoding) as f:
            json.dump(res, f, indent="\t")
            
        print('[ - ] JSON saved at '+ saveLocation+fileName)
        
        return saveLocation+fileName, res
        
if __name__ == "__main__":
    server = '210.89.189.106'
    username = 'ws_admin'
    password = 'gkftndlTek0815'
    db = 'ws_datas'
    collection = 'git_test'
    keyField = 'lang'

    # connect
    db = MongoConnector(db, collection)
    db.connect()
    # setKeyField
    db.setKeyField(keyField)
    
    info = {'keyword':'test',
            'lang':'C',
            'count':0}
    # insert
    #db.insertMany(info)
    db.insertOne(info)
    
    # update
    targetField = 'lang'
    targetCont = 'C'
    #updateField = 'count'
    #updateCont = 1
    
    #db.updateOne(targetField,targetCont,updateField,updateCont)
    # len
    #db.len()
    # delete all
    ##db.deleteAll()

    nf = {'siteName':'github',
          'siteUrl':'http://github.com' 
         }
    db.updateMany(targetField, targetCont, nf)

    # find
    #db.find()
    #db.find('lang','C')

    #db.makeCSV()
    #db.makeCSV(targetField='lang',
    #           targetCont='C',
    #           columnList=['lang','count'],
    #           saveLocation='../',
    #           encoding='utf-8')

    #db.findStringContains('Title', 'Method')
    #db.deleteOne('lang','C')
    
    #db.makeJSON(dateColName='Date')