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
#Helper Functions

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

def recursiveFinder(data, key):
    if key in data:
        return data[key]
    for k,v in data.items():
        if(isinstance(v, dict)):
            item = recursiveFinder(v, key)
            if item is not None:
                return item

# Filtering returned body structure

def singleAggregationData(elasticData, firstKey): 
    if (firstKey != "occupancyValue"):
        ## Anything but occupancy value
        parent = firstKey + "_categories"
        data = {
            parent: []
        }
        list = recursiveFinder(elasticData, "buckets")
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

def singleAggregationFilterData(elasticData, first):
    data = {}
    data.update(elasticData["aggregations"])
    return data

def doubleAggregationData(elasticData, firstKey, secondKey):
    if(firstKey != "occupancyValue" and firstKey != "captureTime"):
        if(secondKey != "occupancyValue"):
            parent = secondKey + "_categories"
            subparent = firstKey + "_categories"
            data = {
                parent: []
            }
            listOuter = recursiveFinder(elasticData, "buckets")
            if(secondKey != "captureTime"):
                ## organizationId or sensorId or systemGuid
                for item in listOuter:
                    innerData = {
                    }
                    item[secondKey] = item.pop("key")
                    listInner = recursiveFinder(item, "buckets")
                    innerData.update({secondKey: item[secondKey]})
                    innerData.update({"doc_count": item['doc_count']})
                    innerData.update({subparent: []})
                    for innerItem in listInner:
                        innerItem[firstKey] = innerItem.pop("key")
                        innerData[subparent].append(innerItem)
                    data[parent].append(innerData)
            else:
                ## captureTime
                for item in listOuter:
                    innerData = {
                    }
                    if("from_as_string" in item):
                        ## data_range type
                        item.pop("key")
                        item.pop("from")
                        item.pop("to")
                        start = item["from_as_string"]
                        end = item["to_as_string"]
                        item.pop("from_as_string")
                        item.pop("to_as_string")
                        item["from"] = inverseDateConvertor(start)
                        item["to"] = inverseDateConvertor(end)
                        listInner = recursiveFinder(item, "buckets")
                        innerData.update({"doc_count": item["doc_count"]})
                        innerData.update({"from": item["from"]})
                        innerData.update({"to": item["to"]})
                        innerData.update({subparent: []})
                        for innerItem in listInner:
                            print(innerItem)
                            innerItem[firstKey] = innerItem.pop("key")
                            innerData[subparent].append(innerItem)
                        data[parent].append(innerData)
                    else:
                        ## date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        listInner = recursiveFinder(item, "buckets")
                        innerData.update({"doc_count": item["doc_count"]})
                        innerData.update({"timestamp": item["timestamp"]})
                        innerData.update({subparent: []})
                        for innerItem in listInner:
                            innerItem[firstKey] = innerItem.pop("key")
                            innerData[subparent].append(innerItem)         
                        data[parent].append(innerData)
    elif(firstKey == "captureTime"):
        parent = secondKey + "_categories"
        subparent = firstKey + "_categories"
        data = {
            parent: []
        }
        if(secondKey == "organizationId" or secondKey == "systemGuid" or secondKey == "sensorId"):
            listOuter = recursiveFinder(elasticData, "buckets")
            print(listOuter)
            for item in listOuter :
                print(item)
                innerData = {

                }
                item[secondKey] = item.pop("key")
                innerData.update({secondKey: item[secondKey]})
                innerData.update({"doc_count": item['doc_count']})
                innerData.update({subparent: []})
                listInner = recursiveFinder(item, "buckets")
                for innerItem in listInner:
                    if "from_as_string" in innerItem:
                        ## date_range type
                        innerItem.pop("key")
                        innerItem.pop("from")
                        innerItem.pop("to")
                        start = innerItem["from_as_string"]
                        end = innerItem["to_as_string"]
                        innerItem.pop("from_as_string")
                        innerItem.pop("to_as_string")
                        innerItem["from"] = inverseDateConvertor(start)
                        innerItem["to"] = inverseDateConvertor(end)
                    else:
                        ## date_histogram type
                        innerItem.pop("key")
                        innerItem["timestamp"] = inverseDateConvertor(innerItem["key_as_string"])
                        innerItem.pop("key_as_string")
                    innerData[subparent].append(innerItem)
                data[parent].append(innerData)
    elif(firstKey == "occupancyValue"):
        parent = secondKey + "_categories"
        subparent = firstKey + "_categories"
        data = {
            parent: []
        }
        listOuter = recursiveFinder(elasticData, "buckets")
        if(secondKey == "captureTime"):
            for item in listOuter:
                innerData = {
                }
                if("from_as_string" in item):
                    ## data_range type
                    item.pop("key")
                    item.pop("from")
                    item.pop("to")
                    start = item["from_as_string"]
                    end = item["to_as_string"]
                    item.pop("from_as_string")
                    item.pop("to_as_string")
                    item["from"] = inverseDateConvertor(start)
                    item["to"] = inverseDateConvertor(end)
                    innerData.update({"doc_count": item["doc_count"]})
                    innerData.update({"from": item["from"]})
                    innerData.update({"to": item["to"]})
                else:
                    ## date_histogram type
                    item.pop("key")
                    item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                    item.pop("key_as_string")
                    innerData.update({"doc_count": item["doc_count"]})
                    innerData.update({"timestamp": item["timestamp"]})
                innerData.update({firstKey: item[firstKey]["value"]})        
                data[parent].append(innerData)
        elif(secondKey == "organizationId" or secondKey == "sensorId" or secondKey == "sysstemGuid"):
            for item in listOuter:
                innerData = {}
                print(item)
                item[secondKey] = item.pop("key")
                innerData.update({secondKey: item[secondKey]})
                innerData.update({"doc_count": item['doc_count']})
                innerData.update({firstKey: item[firstKey]["value"]})
                data[parent].append(innerData)
    return data

def doubleAggregationFilterData(elasticData, firstKey, secondKey):
    if(firstKey == "occupancyValue" and secondKey == "captureTime"):
        parent = secondKey + "_categories"
        subparent = firstKey + "_categories"
        data = {
            parent: []
        }
        listOuter = recursiveFinder(elasticData, "buckets")
        if(secondKey == "captureTime"):
            for item in listOuter:
                innerData = {
                }
                if("from_as_string" in item):
                    ## data_range type
                    item.pop("key")
                    item.pop("from")
                    item.pop("to")
                    start = item["from_as_string"]
                    end = item["to_as_string"]
                    item.pop("from_as_string")
                    item.pop("to_as_string")
                    item["from"] = inverseDateConvertor(start)
                    item["to"] = inverseDateConvertor(end)
                    innerData.update({"doc_count": item["doc_count"]})
                    innerData.update({"from": item["from"]})
                    innerData.update({"to": item["to"]})
                else:
                    ## date_histogram type
                    item.pop("key")
                    item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                    item.pop("key_as_string")
                    innerData.update({"doc_count": item["doc_count"]})
                    innerData.update({"timestamp": item["timestamp"]})
                innerData.update({firstKey: item[firstKey]["doc_count"]})        
                data[parent].append(innerData)
    elif((firstKey == "organizationId" or firstKey == "sensorId" or firstKey == "systemGuid") and secondKey=="captureTime"):
        parent = secondKey + "_categories"
        subparent = firstKey + "_categories"
        data = {
            parent: []
        }
        listOuter = recursiveFinder(elasticData, "buckets")
        for item in listOuter:
            innerData = {
            }
            if("from_as_string" in item):
                ## data_range type
                item.pop("key")
                item.pop("from")
                item.pop("to")
                start = item["from_as_string"]
                end = item["to_as_string"]
                item.pop("from_as_string")
                item.pop("to_as_string")
                item["from"] = inverseDateConvertor(start)
                item["to"] = inverseDateConvertor(end)
                innerData.update({"doc_count": item["doc_count"]})
                innerData.update({"from": item["from"]})
                innerData.update({"to": item["to"]})
                innerData.update({subparent: []})
                innerData[subparent].append(item[firstKey])
                data[parent].append(innerData)
            else:
                ## date_histogram type
                item.pop("key")
                item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                item.pop("key_as_string")
                innerData.update({"doc_count": item["doc_count"]})
                innerData.update({"timestamp": item["timestamp"]})
                innerData.update({subparent: []})
                innerData[subparent].append(item[firstKey])         
                data[parent].append(innerData)
    elif(firstKey == "captureTime"):
        # data = {}
        # data.update(elasticData["aggregations"])
        # data[firstKey]["categories"] = data[firstKey].pop("buckets")
        data = {}
        data.update(elasticData["aggregations"])
        data[secondKey][firstKey]["categories"] = data[secondKey][firstKey].pop("buckets")
        for item in data[secondKey][firstKey]["categories"]:
            if "key_as_string" in item:
                ##date_histogram
                item.pop("key")
                item.update({"timestamp": inverseDateConvertor(item["key_as_string"])})
                item.pop("key_as_string")
            else:
                ##date_range
                item.pop("from")
                item.pop("to")
                item.update({"from": inverseDateConvertor(item["from_as_string"])})
                item.update({"to": inverseDateConvertor(item["to_as_string"])})
                item.pop("from_as_string")
                item.pop("to_as_string")
                item.pop("key")
        return data 
    else:
        data = {}
        data.update(elasticData["aggregations"])
    return data

def tripleAggregationData(elasticData, first, second, third):
    if(first == "organizationId" or first == "sensorId" or first == "systemGuid"):
        if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        middleItem[second] = middleItem.pop("key")
                        middleData.update({second: middleItem[second]})
                        middleData.update({'doc_count': middleItem["doc_count"]})
                        middleData.update({innerparent: []})
                        listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            innerItem[first] = innerItem.pop("key")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if ("from_as_string" in item):
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
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                        for middleItem in listMiddle:
                            middleData = {}
                            middleItem[second] = middleItem.pop("key")
                            middleData.update({second: middleItem[second]})
                            middleData.update({'doc_count': middleItem["doc_count"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                            for innerItem in listInner:
                                innerItem[first] = innerItem.pop("key")
                                middleData[innerparent].append(innerItem)
                            outerData[subparent].append(middleData)
                        data[parent].append(outerData)
                    else:
                        ##date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                        for middleItem in listMiddle:
                            middleData = {}
                            middleItem[second] = middleItem.pop("key")
                            middleData.update({second: middleItem[second]})
                            middleData.update({'doc_count': middleItem["doc_count"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                            for innerItem in listInner:
                                innerItem[first] = innerItem.pop("key")
                                middleData[innerparent].append(innerItem)
                            outerData[subparent].append(middleData)
                        data[parent].append(outerData)
        elif(second == "captureTime"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                            for innerItem in listInner:
                                innerItem[first] = innerItem.pop("key")
                                middleData[innerparent].append(innerItem)
                            outerData[subparent].append(middleData)
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                            for innerItem in listInner:
                                innerItem[first] = innerItem.pop("key")
                                middleData[innerparent].append(innerItem)
                            outerData[subparent].append(middleData)
                        data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if "from_as_string" in item:
                        ##date_range type
                        item.pop("key")
                        item.pop("from")
                        item.pop("to")
                        start = item["from_as_string"]
                        end = item["to_as_string"]
                        item.pop("from_as_string")
                        item.pop("to_as_string")
                        item["from"] = inverseDateConvertor(start)
                        item["to"] = inverseDateConvertor(end)
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    else:
                        #date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                            middleData.update({innerparent: []})
                            listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            innerItem[first] = innerItem.pop("key")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
    elif(first == "occupancyValue"):
        if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        middleItem[second] = middleItem.pop("key")
                        middleData.update({second: middleItem[second]})
                        middleData.update({'doc_count': middleItem["doc_count"]})
                        middleData.update({first: middleItem[first]["value"]})
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if ("from_as_string" in item):
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
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    else:
                        ##date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        middleItem[second] = middleItem.pop("key")
                        middleData.update({second: middleItem[second]})
                        middleData.update({'doc_count': middleItem["doc_count"]})
                        listInner = recursiveFinder(middleItem, "buckets")
                        middleData.update({first: middleItem[first]["value"]})
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
        elif(second == "captureTime"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            print(middleItem)
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                        middleData.update({first: middleItem[first]["value"]})
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if ("from_as_string" in item):
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
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    else:
                        ##date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                        listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                        middleData.update({first: middleItem[first]["value"]})
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData) 
    elif(first == "captureTime"):
        if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        middleItem[second] = middleItem.pop("key")
                        middleData.update({second: middleItem[second]})
                        middleData.update({'doc_count': middleItem["doc_count"]})
                        middleData.update({innerparent: []})
                        listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            if("from_as_string" in innerItem):
                                ##date_range type
                                innerItem.pop("key")
                                innerItem.pop("from")
                                innerItem.pop("to")
                                start = innerItem["from_as_string"]
                                end = innerItem["to_as_string"]
                                innerItem.pop("from_as_string")
                                innerItem.pop("to_as_string")
                                innerItem["from"] = inverseDateConvertor(start)
                                innerItem["to"] = inverseDateConvertor(end)
                            else:
                                ##date_histogram type
                                innerItem.pop("key")
                                innerItem["timestamp"] = inverseDateConvertor(innerItem["key_as_string"])
                                innerItem.pop("key_as_string")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if ("from_as_string" in item):
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
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                    else:
                        ##date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        middleItem[second] = middleItem.pop("key")
                        middleData.update({second: middleItem[second]})
                        middleData.update({'doc_count': middleItem["doc_count"]})
                        middleData.update({innerparent: []})
                        listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            if("from_as_string" in innerItem):
                                ##date_range type
                                innerItem.pop("key")
                                innerItem.pop("from")
                                innerItem.pop("to")
                                start = innerItem["from_as_string"]
                                end = innerItem["to_as_string"]
                                innerItem.pop("from_as_string")
                                innerItem.pop("to_as_string")
                                innerItem["from"] = inverseDateConvertor(start)
                                innerItem["to"] = inverseDateConvertor(end)
                            else:
                                ##date_histogram type
                                innerItem.pop("key")
                                innerItem["timestamp"] = inverseDateConvertor(innerItem["key_as_string"])
                                innerItem.pop("key_as_string")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
        elif(second == "captureTime"):
            if(third == "organizationId" or third == "sensorId" or third == "systemGuid"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    item[third] = item.pop("key")
                    outerData.update({third: item[third]})
                    outerData.update({'doc_count': item["doc_count"]})
                    outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                        middleData.update({innerparent: []})
                        listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            if("from_as_string" in innerItem):
                                ##date_range type
                                innerItem.pop("key")
                                innerItem.pop("from")
                                innerItem.pop("to")
                                start = innerItem["from_as_string"]
                                end = innerItem["to_as_string"]
                                innerItem.pop("from_as_string")
                                innerItem.pop("to_as_string")
                                innerItem["from"] = inverseDateConvertor(start)
                                innerItem["to"] = inverseDateConvertor(end)
                            else:
                                ##date_histogram type
                                innerItem.pop("key")
                                innerItem["timestamp"] = inverseDateConvertor(innerItem["key_as_string"])
                                innerItem.pop("key_as_string")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
            elif(third == "captureTime"):
                parent = third + "_categories"
                subparent = second + "_categories"
                innerparent = first + "_categories"
                data = {
                    parent: []
                }
                listOuter = recursiveFinder(elasticData, "buckets")
                for item in listOuter:
                    outerData = {}
                    if ("from_as_string" in item):
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
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"from": item["from"]})
                        outerData.update({"to": item["to"]})
                        outerData.update({subparent: []})
                    else:
                        ##date_histogram type
                        item.pop("key")
                        item["timestamp"] = inverseDateConvertor(item["key_as_string"])
                        item.pop("key_as_string")
                        outerData.update({"doc_count": item["doc_count"]})
                        outerData.update({"timestamp": item["timestamp"]})
                        outerData.update({subparent: []})
                    listMiddle = recursiveFinder(item, "buckets")
                    for middleItem in listMiddle:
                        middleData = {}
                        if("from_as_string" in middleItem):
                            ## date_range type
                            middleItem.pop("key")
                            middleItem.pop("from")
                            middleItem.pop("to")
                            start = middleItem["from_as_string"]
                            end = middleItem["to_as_string"]
                            middleItem.pop("from_as_string")
                            middleItem.pop("to_as_string")
                            middleItem["from"] = inverseDateConvertor(start)
                            middleItem["to"] = inverseDateConvertor(end)
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"from": middleItem["from"]})
                            middleData.update({"to": middleItem["to"]})
                        else:
                            ##date_histogram type
                            middleItem.pop("key")
                            middleItem["timestamp"] = inverseDateConvertor(middleItem["key_as_string"])
                            middleItem.pop("key_as_string")
                            middleData.update({"doc_count": middleItem["doc_count"]})
                            middleData.update({"timestamp": middleItem["timestamp"]})
                        middleData.update({innerparent: []})
                        listInner = recursiveFinder(middleItem, "buckets")
                        for innerItem in listInner:
                            if("from_as_string" in innerItem):
                                ##date_range type
                                innerItem.pop("key")
                                innerItem.pop("from")
                                innerItem.pop("to")
                                start = innerItem["from_as_string"]
                                end = innerItem["to_as_string"]
                                innerItem.pop("from_as_string")
                                innerItem.pop("to_as_string")
                                innerItem["from"] = inverseDateConvertor(start)
                                innerItem["to"] = inverseDateConvertor(end)
                            else:
                                ##date_histogram type
                                innerItem.pop("key")
                                innerItem["timestamp"] = inverseDateConvertor(innerItem["key_as_string"])
                                innerItem.pop("key_as_string")
                            middleData[innerparent].append(innerItem)
                        outerData[subparent].append(middleData)
                    data[parent].append(outerData)
    return data

def searchData(elasticData, type):
    parent = {
        "result": {
            "total": "",
            "data": []
        }
    }
    hitsList = elasticData["hits"]["hits"]
    parent["result"]["total"] = elasticData["hits"]["total"]
    for hit in hitsList:
        hit["_source"]["captureTime"] = inverseDateConvertor(hit["_source"]["captureTime"])
        parent["result"]["data"].append(hit["_source"])
    return parent

#Aggregation Logic
class SingleAggregation(Resource):
    def get(self, index, first):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        parser.add_argument("size")
        parser.add_argument("value")
        args = parser.parse_args()
        if(args["from"] != None and args["to"] != None):
            dates = dateConvertor(args["from"], args["to"])
            args["from"] = dates[0]
            args["to"] = dates[1]
        if(args["value"] != None):
            values = args["value"].split(',')
            print(values)
            if(len(values) > 1):
                filterAgg = "terms"
            else:
                filterAgg = "term"
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {
            'Content-type': 'application/json'
        }
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
        if(args["value"] != None):
            if(args["type"] != None):
                aggType = args["type"]
            else:
                aggType = "terms"
            body["aggs"][first].pop(aggType)
            if(filterAgg == "term"):
                filterAggregation = {
                    filterAgg: {
                        first: args["value"]
                    }
                }
            else:
                filterAggregation = {
                    filterAgg: {
                        first: values
                    }
                }
            body["aggs"][first].update({"filter": filterAggregation})
        body = str(body).replace("'", '"')
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        if(args["value"] != None):
            return singleAggregationFilterData(json.loads(response.read().decode()), first)
        else:
            return singleAggregationData(json.loads(response.read().decode()), first)
        connection.close()

class DoubleAggregation(Resource):
    def get(self, index, first, second):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        parser.add_argument("occupancyValue_value")
        parser.add_argument("sensorId_value")
        parser.add_argument("organizationId_value")
        parser.add_argument("systemGuid_value")
        parser.add_argument("captureTime_value")
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
            elif(second == "occupancyValue" and args["occupancyValue_value"] == None):
                error = {
                    "Error": "Invalid sub-aggregation"
                }
                return json.loads(str(error).replace("'", '"'))
            elif(second == "captureTime"):
                aggType = args["type"]
                if(args[first+"_value"] == None):
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
                else:
                    values = args[first + "_value"].split(',')
                    if(len(values) > 1):
                        filterAgg = "terms"
                    else:
                        filterAgg = "term"
                        values = values[0]
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
                                            "filter":{
                                                filterAgg:{
                                                    first: values
                                                }
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
                                            "filter":{
                                                filterAgg:{
                                                    first: values
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
        elif(first == "occupancyValue"):
            if(second == "organizationId" or second == "sensorId" or second == "systemGuid"):
                if(args[first+"_value"] == None):
                    if(args["type"] != None):
                        aggType = args["type"]
                    else:
                        aggType = "term"
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
                if(len(types) == 1):
                    if types[0] == "date_histogram":
                        body = {
                            "size": 0,
                            "aggs": {
                                second:{
                                    types[0]:{
                                        "field": second,
                                        "interval": args["interval"]
                                    },
                                    "aggs":{
                                        first:{
                                            "filter":{
                                                "term":{
                                                    first: args[first+"_value"]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    elif types[0] == "date_range":
                        print(types)
                        body = {
                            "size": 0,
                            "aggs": {
                                second:{
                                    types[0]:{
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
                                            "filter":{
                                                "term":{
                                                    first: args[first+"_value"]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                else:
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
        if(args[second+"_value"] != None and args[first+"_value"] == None):
            values = args[second + "_value"].split(',')
            if(len(values) > 1):
                filterAgg = "terms"
            else:
                filterAgg = "term"
            if(args["type"] == None):
                aggType = "terms"
            else:
                aggType = args["type"]
            print(body)
            secondaryAgg = body["aggs"][second]["aggs"]
            body["aggs"][second].pop("terms")
            body["aggs"][second].pop("aggs")
            if(filterAgg == "term"):
                filterAggregation = {
                    filterAgg: {
                        second: args[second + "_value"]
                    }
                }
            else:
                filterAggregation = {
                    filterAgg: {
                        second: values
                    }
                }
            body["aggs"][second].update({"filter": filterAggregation})
            body["aggs"][second].update({"aggs": secondaryAgg})
            print(body)
        elif(args[second+"_value"]!=None and args[first+"_value"]!=None):
            valuesSecond = args[second + "_value"].split(',')
            valuesFirst = args[first + "_value"].split(',')
            if(len(valuesSecond) > 1):
                filterAggSecond = "terms"
            else:
                filterAggSecond = "term"
                valuesSecond = valuesSecond[0]
            if(len(valuesFirst) > 1):
                filterAggFirst = "terms"
            else:
                filterAggFirst = "term"
                valuesFirst = valuesFirst[0]
            if(args["type"] == None):
                aggType = "terms"
            else:
                aggType = args["type"]
            body = {
                "size": 0,
                "aggs":{
                    second:{
                        "filter":{
                            filterAggSecond:{
                                second: valuesSecond
                            }
                        },
                        "aggs":{
                            first:{
                                "filter":{
                                    filterAggFirst:{
                                        first: valuesFirst
                                    }
                                }
                            }
                        }
                    }
                }
            }
        elif(args[second+"_value"]==None and args[first+"_value"]!=None):
            if(second!="captureTime"):
                values = args[first + "_value"].split(',')
                if(len(values) > 1):
                    filterAgg = "terms"
                else:
                    filterAgg = "term"
                    values = values[0]
                if(args["type"] == None):
                    aggType = "terms"
                else:
                    aggType = args["type"]
                body = {
                    "size": 0,
                    "aggs":{
                        second:{
                            aggType:{
                                "field": second 
                            },
                            "aggs":{
                                first:{
                                    "filter":{
                                        filterAgg:{
                                            first: values
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

        body = str(body).replace("'", '"')
        print(body)
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        if(args[second + "_value"] != None or args[first + "_value"] != None):
            print("hit")
            return doubleAggregationFilterData(json.loads(response.read().decode()), first, second)
        else:
            return doubleAggregationData(json.loads(response.read().decode()), first, second)
        # return json.loads(response.read().decode())
        connection.close()

class TripleAggregation(Resource):
    def get(self, index, first, second, third):
        parser = reqparse.RequestParser()
        parser.add_argument("type")
        parser.add_argument("from")
        parser.add_argument("to")
        parser.add_argument("interval")
        parser.add_argument("sensorId_value")
        parser.add_argument("systemGuid_value")
        parser.add_argument("organizationId_value")
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
                                            aggType:{
                                                "field": second,
                                                "interval": args["interval"]
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
                                                first:{
                                                    aggType:{
                                                        "field": second,
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

        body = str(body).replace("'", '"')
        # print(body)
        connection.request("GET", index + "/_search", body, headers)
        response = connection.getresponse()
        return tripleAggregationData(json.loads(response.read().decode()), first, second, third)
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
        return searchData(json.loads(response.read().decode()), args["type"])
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