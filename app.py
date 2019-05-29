from flask import Flask
from flask_restful import Api, Resource, reqparse
import json
import http.client

app = Flask(__name__)
api = Api(app)

#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#

def dateConvertor(start, end):
    list = []
    startTimestamp = start.split('-')
    startDate = startTimestamp[0].split('/')
    startTime = startTimestamp[1].split(':')
    beginTimestamp = "".join(startDate[::-1]) + "T" + "".join(startTime) + "+0000"
    list.append(beginTimestamp)
    endTimestamp = end.split('-')
    endDate = endTimestamp[0].split('/')
    endTime = endTimestamp[1].split(':')
    endingTimestamp = "".join(endDate[::-1]) + "T" + "".join(endTime) + "+0000"
    list.append(endingTimestamp)
    return list

def inverseDateConvertor(start):
    startList = start.split('T')
    newStartDate = startList[0][6:8] + '/' + startList[0][4:6] + '/' + startList[0][:4]
    newStartTime = startList[1][:2] + ':' + startList[1][2:4] + ':' + startList[1][4:6]
    startList = [newStartDate, newStartTime]
    startTimestamp = '-'.join(startList)
    return startTimestamp

def recursiveFinder(data):
    if "buckets" in data:
        return data["buckets"]
    for k,v in data.items():
        if(isinstance(v, dict)):
            item = recursiveFinder(v)
            if item is not None:
                return item

def singleAggregationData(elasticData, firstKey): 
    if (firstKey != "occupancyValue"):
        ## Anything but occupancy value
        parent = firstKey + "_categories"
        data = {
            parent: []
        }
        list = recursiveFinder(elasticData)
        if(firstKey != "captureTime"):
            ## organizationId or sensorId or systemGuid
            for item in list:
                if "key" in item:
                    item[firstKey] = item.pop("key")
                    data[parent].append(item)
        else:
            ## captureTime
            for item in list:
                if("from_as_string" in item):
                    ## date_range type
                    item.pop("key")
                    item.pop("from")
                    item.pop("to")
                    start = item["from_as_string"]
                    end = item["to_as_string"]
                    item.pop("from_as_string")
                    item.pop("to_as_string")
                    item["from"] = inverseDateConvertor(start)
                    item["to"] = inverseDateConvertor(end)
                    data[parent].append(item)
                else:
                    ## date_histogram type
                    item.pop("key")
                    item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                    item.pop("key_as_string")
                    data[parent].append(item)
    else:
        data = {}
        data.update(elasticData["aggregations"])
    return data


##[yyyyMMdd'T'HHmmssZ]

#Aggregation Logic
class SingleAggregation(Resource):
    def get(self, index, first):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        parser.add_argument("size")
        args = parser.parse_args()
        if(args["from"] != None and args["to"] != None):
            dates = dateConvertor(args["from"], args["to"])
            args["from"] = dates[0]
            args["to"] = dates[1]
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        if(first == "organizationId" or first == "sensorId" or first == "systemGuid"):
            body = {
                "size": 0,
                "aggs":{
                    first:{
                        "terms":{
                            "field": first
                        }
                    }
                }
            }
        elif(first == "occupancyValue"):
            aggType = args["type"]
            body = {
                "size": 0,
                "aggs":{
                    first:{
                        aggType:{
                            "field": first
                        }
                    }
                }
            }
        elif(first == "captureTime"):
            aggType = args["type"]
            if(aggType == "date_histogram"):
                body = {
                    "size": 0,
                    "aggs":{
                        first:{
                            aggType:{
                                "field": first,
                                "interval": args["interval"]
                            }
                        }
                    }
                }
            elif(aggType == "date_range"):
                body = {
                    "size": 0,
                    "aggs":{
                        first:{
                            aggType:{
                                "field": first,
                                "ranges":[
                                    {
                                        "from": args["from"],
                                        "to": args["to"]
                                    }
                                ]
                            }
                        }
                    }
                }
        body = str(body).replace("'", '"')
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        return singleAggregationData(json.loads(response.read().decode()), first)
        connection.close()

class DoubleAggregation(Resource):
    def get(self, index, first, second):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        args = parser.parse_args()
        if(args["from"] != None and args["to"] != None):
            dates = dateConvertor(args["from"], args["to"])
            args["from"] = dates[0]
            args["to"] = dates[1]
        if(args["type"] != None):
            types = args["type"].split(',')
            # print(types)
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        if(first == "organizationId" or first == "sensorId" or first == "systemGuid"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                body = {
                        "size": 0,
                        "aggs":{
                            second:{
                                "terms":{
                                    "field": second
                                },
                                "aggs":{
                                    first:{
                                        "terms":{
                                            "field": first
                                        }
                                    }
                                }
                            }
                        }
                    }
            elif(second == "occupancyValue"):
                error = {
                    "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                aggType = args["type"]
                if(aggType == "date_range"):
                    body = {
                        "size": 0,
                        "aggs":{
                            second:{
                                aggType:{
                                    "field": second,
                                    "ranges":[
                                        {
                                            "from": args["from"],
                                            "to": args["to"]
                                        }
                                    ]
                                },
                                "aggs":{
                                    first:{
                                        "terms":{
                                            "field": first
                                        }
                                    }
                                }
                            }
                        }
                    }
                elif(aggType == "date_histogram"):
                    body = {
                        "size": 0,
                        "aggs":{
                            second:{
                                aggType:{
                                    "field": second,
                                    "interval": args["interval"]
                                },
                                "aggs":{
                                    first:{
                                        "terms":{
                                            "field": first
                                        }
                                    }
                                }
                            }
                        }
                    }
        elif(first == "occupancyValue"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                aggType = args["type"]
                body = {
                    "size": 0,
                    "aggs":{
                        second:{
                            "terms":{
                                "field": second
                            },
                            "aggs":{
                                first:{
                                    aggType:{
                                        "field": first
                                    }
                                }
                            }
                        }
                    }
                }
            elif(second == "captureTime"):
                if types[1] == "date_histogram":
                    body = {
                        "size": 0,
                        "aggs": {
                            second:{
                                types[1]:{
                                    "field": second,
                                    "interval": args["interval"]
                                },
                                "aggs":{
                                    first:{
                                        types[0]:{
                                            "field": first
                                        }
                                    }
                                }
                            }
                        }
                    }
                elif types[1] == "date_range":
                    body = {
                        "size": 0,
                        "aggs": {
                            second:{
                                types[1]:{
                                    "field": second,
                                    "ranges":[
                                        {
                                            "from": args["from"],
                                            "to": args["to"]
                                        }
                                    ]
                                },
                                "aggs":{
                                    first:{
                                        types[0]:{
                                            "field": first
                                        }
                                    }
                                }
                            }
                        }
                    }
            elif(second == "occupancyValue"):
                error = {
                    "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
        elif(first == "captureTime"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                aggType = args["type"]
                if(aggType == "date_histogram"):
                    body = {
                        "size": 0,
                        "aggs": {
                            second:{
                                "terms":{
                                    "field": second
                                },
                                "aggs":{
                                    first:{
                                        aggType:{
                                            "field": first,
                                            "interval": args["interval"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                elif(aggType == "date_range"):
                    body = {
                        "size": 0,
                        "aggs": {
                            second:{
                                "terms":{
                                    "field": second
                                },
                                "aggs":{
                                    first:{
                                        aggType:{
                                            "field": first,
                                            "ranges":[
                                                {
                                                    "from": args["from"],
                                                    "to": args["to"]
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
            elif(second == "occupancyValue"):
                error = {
                    "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                error = {
                    "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
        body = str(body).replace("'", '"')
        # print(body)
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        return singleAggregationData(json.loads(response.read().decode()), second)
        # return json.loads(response.read().decode())
        connection.close()

class TripleAggregation(Resource):
    def get(self, index, first, second, third):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        args = parser.parse_args()
        if(args["from"] != None and args["to"] != None):
            dates = dateConvertor(args["from"], args["to"])
            args["from"] = dates[0]
            args["to"] = dates[1]
        if(args["type"] != None):
            types = args["type"].split(',')
            # print(types)
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}

        if(first == "organizationId" or first == "sensorId" or first == "systemGuid"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    body = {
                        "size": 0,
                        "aggs":{
                            third:{
                                "terms":{
                                    "field": third
                                },
                                "aggs":{
                                    second:{
                                        "terms":{
                                            "field": second
                                        },
                                        "aggs":{
                                            first:{
                                                "terms":{
                                                    "field": first
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    aggType = args["type"]
                    if(aggType == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                first:{
                                                    "terms":{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(aggType == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                third:{
                                                    "terms":{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
            elif(second == "occupancyValue"):
                error = {
                        "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    aggType = args["type"]
                    if(aggType == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            aggType:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    "terms":{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(aggType == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            aggType:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    "terms":{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    aggType = args["type"]
                    if(aggType == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            aggType:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    "terms":{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(aggType == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            aggType:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    "terms":{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
        elif(first == "occupancyValue"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    aggType = args["type"]
                    body = {
                        "size": 0,
                        "aggs":{
                            third:{
                                "terms":{
                                    "field": third
                                },
                                "aggs":{
                                    second:{
                                        "terms":{
                                            "field": second
                                        },
                                        "aggs":{
                                            first:{
                                                aggType:{
                                                    "field": first
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    if(types[1] == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(types[1] == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
            elif(second == "occupancyValue"):
                error = {
                        "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    if(types[1] == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(types[1] == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    types[0]:{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    if(types[1] == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(types[1] == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    types[0]:{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
        elif(first == "captureTime"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    aggType = args["type"]
                    if(aggType == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                first:{
                                                    aggType:{
                                                        "field": first,
                                                        "interval": args["interval"]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(aggType == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                third:{
                                                    aggType:{
                                                        "field": third,
                                                        "ranges":[
                                                            {
                                                                "from": args["from"],
                                                                "to": args["to"]
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }               
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    aggType = args["type"]
                    if(aggType == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                first:{
                                                    aggType: {
                                                        "field": first,
                                                        "interval": args["interval"]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(aggType == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    aggType:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            "terms":{
                                                "field": second
                                            },
                                            "aggs":{
                                                third:{
                                                    aggType:{
                                                        "field": third,
                                                        "ranges":[
                                                            {
                                                                "from": args["from"],
                                                                "to": args["to"]
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
            elif(second == "occupancyValue"):
                error = {
                        "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                    if(types[1] == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(types[1] == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    "terms":{
                                        "field": third
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    types[0]:{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                elif(third == "occupancyValue"):
                    error = {
                        "Error": "Invalid sub-aggregation"
                    }
                    return json.loads(str(error).replace("'", '"'))
                elif(third == "captureTime"):
                    if(types[1] == "date_histogram"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "interval": args["interval"]
                                            },
                                            "aggs":{
                                                first:{
                                                    types[0]:{
                                                        "field": first
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif(types[1] == "date_range"):
                        body = {
                            "size": 0,
                            "aggs": {
                                third:{
                                    types[1]:{
                                        "field": third,
                                        "ranges":[
                                            {
                                                "from": args["from"],
                                                "to": args["to"]
                                            }
                                        ]
                                    },
                                    "aggs":{
                                        second:{
                                            types[1]:{
                                                "field": second,
                                                "ranges":[
                                                    {
                                                        "from": args["from"],
                                                        "to": args["to"]
                                                    }
                                                ]
                                            },
                                            "aggs":{
                                                third:{
                                                    types[0]:{
                                                        "field": third
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
        
        body = str(body).replace("'", '"')
        # print(body)
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

#Search Logic
class Search(Resource):
    def get(self, index, field, value):
        connection = http.client.HTTPConnection('10.8.173.181', 80)
        parser = reqparse.RequestParser()
        parser.add_argument("size")
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        parser.add_argument("count")
        args = parser.parse_args()
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        queries = value.split(',')
        if(args["size"] != None):
            args["size"] = int(args["size"])
        if(args["type"] == "term" or args["type"] == "match" or args["type"] == "prefix"):
            if(args["size"] != None and args["size"] >= 0):
                body = {
                    "size": args["size"],
                    "query":{
                        args["type"]: {
                            field: value
                        }
                    }
                }
            else:
                body = {
                    "query":{
                        args["type"]: {
                            field: value
                        }
                    }
                }
        elif(args["type"] == "terms"):
            if(args["size"] != None and args["size"] >= 0):
                body = {
                    "size": args["size"],
                    "query":{
                        args["type"]: {
                            field: queries
                        }
                    }
                }
            else:
                body = {
                    "query":{
                        args["type"]: {
                            field: queries
                        }
                    }
                }
        elif(args["type"] == "exists"):
            body = {
                "query":{
                    args["type"]: {
                        field: value
                    }
                }
            }
        body = str(body).replace("'", '"')
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#

#Aggregation Endpoints
api.add_resource(SingleAggregation, "/<string:index>/aggs/<string:first>")
api.add_resource(DoubleAggregation, "/<string:index>/aggs/<string:first>/<string:second>")
api.add_resource(TripleAggregation, "/<string:index>/aggs/<string:first>/<string:second>/<string:third>")

#Search Endpoints
api.add_resource(Search, "/<string:index>/search/<string:field>/<string:value>")

if __name__ == "__main__":
    app.run()