from elasticsearch import Elasticsearch
from collections import defaultdict, OrderedDict
import operator
import json

with open('topics.json') as f:
  QUERIES = json.load(f)


es = Elasticsearch()

def getResult(query, indexName):
    documentSet = set()

    try:
        res = None
        try:
            res = es.search(index=indexName, body={
                "query": {
                "bool": {  
                    "should": [
                            {
                            "match": {
                                "text":{
                                "query": query
                            }
                            }
                            },
                            {
                            "match": {
                                "text":{
                                "query": query
                                ,"operator": "and"
                                }
                            }
                            }
                        ]
                        }
                    }
,
                "sort": [
                    "_score"
                ],
                'size': 1000
            })
        except Exception as e:
            print(e)

        resultRankedListDict = defaultdict(lambda: 0.0)
        for hit in res['hits']['hits']:
            docno = str(hit['_source']['id'])
            normaldocNo = docno
            if normaldocNo not in documentSet:
                if len(documentSet) == 1000:
                    break
                resultRankedListDict[docno] = float(hit['_score'])
                documentSet.add(normaldocNo)

        return resultRankedListDict


    except Exception as e:
        print(e)


def writeRankedList(resultDict, topicID):
    rank = 1
    with open('NEWBASELINERESULTS.txt', 'a+') as f:
        for docId, score in resultDict.items():
            f.write(str(topicID) + ' ' + 'Q0' + ' '+ docId  + ' ' + str(rank) + ' ' + str(score) + ' '+ 'Trial1-top500\n')
            rank += 1

if __name__ == '__main__':
    for topicID in QUERIES:
        print(topicID)
        desc = str(QUERIES[topicID])
 	#   #print(d)
        ## WHen you need to use only main topics then replace d with str(QUERIES[topicID])
        result = getResult(desc, 'robust04_docprofiler')
        writeRankedList(OrderedDict(sorted(result.items(), key=operator.itemgetter(1), reverse=True)), topicID)


"""

       "query":{
                    "bool":{
                    "should":[
                        {
                            "multi_match":{
                                "query":query,
                                "type":"most_fields",
                                "fields":[
                                "text",
                                #"Tagme-entities^2"
                                "Keyphrases^2"
                                ],"operator": "and"
                            
                            }
                        }
                    ]
                    }
                },
                "sort": [
                    "_score"
                ],
                'size': 1000
"""

"""
"query": {
                    "match": {
                        "text": query
                    }
                },
                "sort": [
                    "_score"
                ],
                'size': 1000
"""

"""
   "query":{
                    "bool":{
                    "should":[
                        {
                            "multi_match":{
                                "query":query,
                                "type":"best_fields",
                                "fields":[
                                "text",
                                "Tagme-entities"
                               # "Keyphrases",
                               # "Summary"
                                ],"operator": "and"
                            }
                        }
                    ]
                    }
                },
                "sort": [
                    "_score"
                ],
                'size': 1000
            })
"""

"""
                    "query": {
                        "bool": {
                        "should": [{
                            "match": {
                                "text":{
                                "query": query,
                                "operator": "and"
                                }}},
                            {
                            "match": {
                                "Tagme-entities":{
                                "query": query,
                                "operator": "and"}}}]}
                            },
                        "sort": [
                    "_score"
                ],
                'size': 1000

"""


"""
 "query": {
                        "multi_match" : {
                        "query": query,
                        "type": "most_fields",
                        "fields": [ "text"],
                        "operator":"and"
                        }
                    }
"""

##Regular match query 

"""
"query": {
                    "match": {
                        "text": query
                    }
                },
                "sort": [
                    "_score"
                ],
                'size': 1000

"""

#Comparision between Most_fields,"Best_fields"

"""
Ultimate result:

            "query": {
                "bool": {  
                    "must": [
                            {
                            "match": {
                                "text":{
                                "query": query
                            }
                            }
                            },
                            {
                            "match": {
                                "text":{
                                "query": query
                                ,"operator": "and"
                                }
                            }
                            },
                            {
                            "match": {
                                "Tagme-entities":{
                                "query": query,
                                "boost": 2
                                ,"operator": "and"
                                }
                            }
                            }
                        ]
                        }
                    }

"""
