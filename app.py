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

class SingleAggregation(Resource):
    def get(self, index, first):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        args = parser.parse_args()
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
        return json.loads(response.read().decode())
        connection.close()

class DoubleAggregation(Resource):
    def get(self, index, first, second):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        args = parser.parse_args()
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
        return json.loads(response.read().decode())
        connection.close()

class TripleAggregation(Resource):
    def get(self, index, first, second, third):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        args = parser.parse_args()
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

#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------#

api.add_resource(SingleAggregation, "/<string:index>/aggs/<string:first>")
api.add_resource(DoubleAggregation, "/<string:index>/aggs/<string:first>/<string:second>")
api.add_resource(TripleAggregation, "/<string:index>/aggs/<string:first>/<string:second>/<string:third>")

if __name__ == '__main__':
    app.run()