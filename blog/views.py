from django.shortcuts import render
from django.utils import timezone
from .models import Post

def post_list(request):
    posts = Post.objects.filter(published_date__lte=timezone.now())
    return render(request, 'blog/post_list.html',{'posts':posts})


# Create your views here.
from django.http import JsonResponse

from . import mongoDB
import datetime

dbNameDict = {'post':'admin',
              'user':'admin'
             }
collectionNameDict = {'post':'twitter_post',
                      'user':'twitter_user'
                     }

def connectTweetDB(sitename, subsite):
    db = mongoDB.MongoConnector(dbNameDict[sitename], collectionNameDict[sitename])
    db.connect()
    # setKeyField
    db.setKeyField('tweetID')

    return db

def generateQuery(request):
    q = {}
    if 'keyword' in list(request.GET.keys()):
        q['content'] = {}
        q['content']['$regex'] = request.GET['keyword']
    if 'from' in list(request.GET.keys()):
        year = int(request.GET['from'][:4])
        month = int(request.GET['from'][4:6])
        day = int(request.GET['from'][6:8])
        q['datePublished'] = {}
        q['datePublished']['$gte'] = datetime.datetime(year, month, day, 0, 0, 0)
    if 'to' in list(request.GET.keys()):
        if 'datePublished' not in list(q.keys()):
            q['datePublished'] = {}
        year = int(request.GET['to'][:4])
        month = int(request.GET['to'][4:6])
        day = int(request.GET['to'][6:8])
        q['datePublished']['$lte'] = datetime.datetime(year, month, day, 23, 59, 59)

    return q

def getData(request, db):
    items = []

    q = generateQuery(request)

    for item in db.collection.find(q):
        infor = {
                'datePublished':item['datePublished'],
                'user':item['user'],
                'userName':item['userName'],
                'content':item['content'],
                'media':item['media'],
                'nLike':item['nFavorite'],
                'nRetweet':item['nRetweet'],
                'nReply':item['nReply']
                }
        items.append(infor)     
    return len(items), items  

def getSortByData(request, db):
    items = []
    q = generateQuery(request)

    if 'sortby' in list(request.GET):
        for item in db.collection.find(q).sort(request.GET['sortby'],-1):
            infor = {
                    'datePublished':item['datePublished'],
                    'user':item['user'],
                    'userName':item['userName'],
                    'content':item['content'],
                    'media':item['media'],
                    'nLike':item['nFavorite'],
                    'nRetweet':item['nRetweet'],
                    'nReply':item['nReply']
                    }
            items.append(infor)     
    return len(items), items  

def rank(request):
    res = {'itemLen':0,
           'items':[]  
          }
    db = connectTweetDB('post','post')

    if 'sortby' in list(request.GET):
        res['itemLen'], res['items'] = getSortByData(request, db)
    else:
        res['itemLen'], res['items'] = getData(request, db)
    return JsonResponse(res, safe=False)    
