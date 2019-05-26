from flask import Flask
from elasticsearch import Elasticsearch
from flask_restful import Api, Resource, reqparse
import json
import http.client

app = Flask(__name__)
api = Api(app)


class Document(Resource):
    def get(self, index, type, id):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        parser = reqparse.RequestParser()
        parser.add_argument("source")
        parser.add_argument("source_includes")
        parser.add_argument("source_excludes")
        parser.add_argument("stored_fields")
        args = parser.parse_args()
        if (args["source"] != None):
            params.append("_source=" + args["source"])
        if (args["source_includes"] != None):
            params.append("_source_includes=" +  args["source_includes"])
        if (args["source_excludes"] != None):
            params.append("_source_excludes=" + args["source_excludes"])
        if (args["stored_fields"] != None):
            params.append("stored_fields=" + args["stored_fields"])
        parameters = "&".join(params)
        url = index + "/" + type + "/" + id + "?" + parameters
        connection.request("GET", url)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

    def post(self, index, type, id):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("retry_on_conflicts")
        parser.add_argument("routing")
        parser.add_argument("timeout")
        parser.add_argument("wait_for_active_shards")
        parser.add_argument("refresh")
        parser.add_argument("_source")
        parser.add_argument("version")
        parser.add_argument("if_seq_no")
        parser.add_argument("if_primary_term")
        args = parser.parse_args()
        if (args["retry_on_conflicts"] != None):
            params.append("retry_on_conflicts=" + args["retry_on_conflicts"])
        if (args["routing"] != None):
            params.append("routing=" +  args["routing"])
        if (args["timeout"] != None):
            params.append("timeout=" + args["timeout"])
        if (args["wait_for_active_shards"] != None):
            params.append("wait_for_active_shards=" + args["wait_for_active_shards"])
        if (args["refresh"] != None):
            params.append("refresh=" + args["refresh"])
        if (args["_source"] != None):
            params.append("_source=" + args["_source"])
        if (args["version"] != None):
            params.append("version=" + args["version"])
        if (args["if_seq_no"] != None):
            params.append("if_seq_no=" + args["if_seq_no"])
        if (args["if_primary_term"] != None):
            params.append("if_primary_term=" + args["if_primary_term"])
        parameters = "&".join(params)
        url = index + "/" + type + "/" + id + "/_update" + "?" + parameters
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

    def put(self, index, type, id):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        body = str(args["data"]).replace("'", '"')
        url = index + "/" + type + "/" + id
        connection.request("PUT", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

    def delete(self, index, type, id):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("DELETE", index + "/" + type + "/" + id)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Source(Resource):
    def get(self, type, index, id):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        parser = reqparse.RequestParser()
        parser.add_argument("source_includes")
        parser.add_argument("source_excludes")
        args = parser.parse_args()
        if (args["source_includes"] != None):
            params.append("_source_includes=" +  args["source_includes"])
        if (args["source_excludes"] != None):
            params.append("_source_excludes=" + args["source_excludes"])
        parameters = "&".join(params)
        url = index + "/" + type + "/" + id + "/_source" + "?" + parameters
        connection.request("GET", url)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class DeleteByQuery(Resource):
    def post(self, index):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("conflicts")
        parser.add_argument("routing")
        parser.add_argument("scroll_size")
        args = parser.parse_args()
        if (args["conflicts"] != None):
            params.append("conflicts=" + args["conflicts"])
        if (args["routing"] != None):
            params.append("routing=" +  args["routing"])
        if (args["scroll_size"] != None):
            params.append("scroll_size=" + args["scroll_size"])
        parameters = "&".join(params)
        url = index + "/_delete_by_query" + "?" + parameters
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class UpdateByQuery(Resource):
    def post(self, index):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("conflicts")
        parser.add_argument("routing")
        parser.add_argument("scroll_size")
        args = parser.parse_args()
        if (args["conflicts"] != None):
            params.append("conflicts=" + args["conflicts"])
        if (args["routing"] != None):
            params.append("routing=" +  args["routing"])
        if (args["scroll_size"] != None):
            params.append("scroll_size=" + args["scroll_size"])
        parameters = "&".join(params)
        url = index + "/_update_by_query" + "?" + parameters
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Bulk(Resource):
    def post(self):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        url = "_bulk"
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Reindex(Resource):
    def post(self):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        url = "_reindex"
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Search(Resource):
    def get(self, index):
        params = []
        connection = http.client.HTTPConnection("10.8.173.181", 80)        
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("q")
        parser.add_argument("df")
        parser.add_argument("analyzer")
        parser.add_argument("analyze_wildcard")
        parser.add_argument("batched_reduce_size")
        parser.add_argument("default_operator")
        parser.add_argument("lenient")
        parser.add_argument("explain")
        parser.add_argument("_source")
        parser.add_argument("stored_fields")
        parser.add_argument("sort")
        parser.add_argument("track_scores")
        parser.add_argument("track_total_hits")
        parser.add_argument("timeout")
        parser.add_argument("terminate_after")
        parser.add_argument("from")
        parser.add_argument("size")
        parser.add_argument("search_type")
        parser.add_argument("allow_partial_search_results")
        args = parser.parse_args()
        headers = {'Content-type': 'application/json'}
        if (args["data"] != None):
            body = str(args["data"]).replace("'", '"')
        if (args["q"] != None):
            params.append("q=" + args["q"])
        if (args["df"] != None):
            params.append("df=" +  args["df"])
        if (args["analyzer"] != None):
            params.append("analyzer=" + args["analyzer"])
        if (args["analyze_wildcard"] != None):
            params.append("analyze_wildcard=" + args["analyze_wildcard"])
        if (args["batched_reduce_size"] != None):
            params.append("batched_reduce_size=" +  args["batched_reduce_size"])
        if (args["default_operator"] != None):
            params.append("default_operator=" + args["default_operator"])
        if (args["lenient"] != None):
            params.append("lenient=" + args["lenient"])
        if (args["explain"] != None):
            params.append("explain=" +  args["explain"])
        if (args["_source"] != None):
            params.append("_source=" + args["_source"])
        if (args["stored_fields"] != None):
            params.append("stored_fields=" +  args["stored_fields"])
        if (args["sort"] != None):
            params.append("sort=" + args["sort"])
        if (args["track_scores"] != None):
            params.append("track_scores=" + args["track_scores"])
        if (args["track_total_hits"] != None):
            params.append("track_total_hits=" +  args["track_total_hits"])
        if (args["timeout"] != None):
            params.append("timeout=" + args["timeout"])
        if (args["terminate_after"] != None):
            params.append("terminate_after=" +  args["terminate_after"])
        if (args["from"] != None):
            params.append("from=" + args["from"])
        if (args["size"] != None):
            params.append("size=" + args["size"])
        if (args["search_type"] != None):
            params.append("search_type=" +  args["search_type"])
        if (args["allow_partial_search_results"] != None):
            params.append("allow_partial_search_results=" + args["allow_partial_search_results"])
        parameters = "&".join(params)
        url = index + "/_search" + "?" + parameters
        if(args["data"] != None):
            connection.request("GET", url, body, headers)
        else:
            connection.request("GET", url)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Aggregation(Resource):
    def get(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)        
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        headers = {'Content-type': 'application/json'}
        body = str(args["data"]).replace("'", '"')
        url = index + "/_search"
        connection.request("GET", url, body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class CreateIndex(Resource):
    def get(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("GET", index)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()
    
    def put(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        if(args["data"] != None):
            body = str(args["data"]).replace("'", '"')
            connection.request("PUT", index, body, headers)
        else:
            connection.request("PUT", index)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

    def delete(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("DELETE", index)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class OpenIndex(Resource):
    def post(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("POST", index + "/_open")
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class CloseIndex(Resource):
    def post(self, index):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("POST", index + "/_close")
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class ShrinkIndex(Resource):
    def post(self, sourceIndex, targetIndex):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("copy_settings")
        args = parser.parse_args()
        if(args["copy_settings"] != None):
            url = sourceIndex + "/_shrink/" + targetIndex + "?copy_settings=" + args["copy_settings"]
        else:
            url = sourceIndex + "/_shrink/" + targetIndex
        
        if(args["data"] != None):
            body = str(args["data"]).replace("'", '"')
            headers = {'Content-type': 'application/json'}
            connection.request("POST", url, body, headers)
        else:
            connection.request("POST", url)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class SplitIndex(Resource):
    def post(self, sourceIndex, targetIndex):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        parser.add_argument("copy_settings")
        args = parser.parse_args()
        if(args["copy_settings"] != None):
            url = sourceIndex + "/_split/" + targetIndex + "?copy_settings=" + args["copy_settings"]
        else:
            url = sourceIndex + "/_split/" + targetIndex
        
        if(args["data"] != None):
            body = str(args["data"]).replace("'", '"')
            headers = {'Content-type': 'application/json'}
            connection.request("POST", url, body, headers)
        else:
            connection.request("POST", url)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Documents(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.search(index=index)
        return res


class Settings(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.indices.get_settings(index=index)
        return res


class Mapping(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.indices.get_mapping(index=index)
        return res


class Mappings(Resource):
    def get(self):
        # es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        # res = es.indices.get_mapping()
        # return res
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("GET", "/_mapping")
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Alias(Resource):
    def get(self, index):
        # es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        # res = es.indices.get_alias(index=index)
        # return res
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("GET", "/_alias/" + index)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class Aliases(Resource):
    def get(self):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        connection.request("GET", "/_aliases")
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

    def post(self):
        connection = http.client.HTTPConnection("10.8.173.181", 80)
        headers = {'Content-type': 'application/json'}
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        body = str(args["data"]).replace("'", '"')
        connection.request("POST", "/_aliases", body, headers)
        response = connection.getresponse()
        return json.loads(response.read().decode())
        connection.close()

class AllAlias(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.aliases(format='json')
        return res


class ClusterHealth(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cluster.health()
        return res


class IndexHealth(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cluster.health(index=index)
        return res


class ClusterStats(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cluster.stats()
        return res


class NodesStats(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.nodes.stats()
        return res


class NodeStats(Resource):
    def get(self, id):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.nodes.stats(node_id=id)
        return res


class Allocation(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.allocation(format='json')
        return res


class AllocationNode(Resource):
    def get(self, name):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.allocation(format='json', node_id=name)
        return res


class Indices(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.indices(format='json')
        return res


class Count(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        body = str(args["data"]).replace("'", '"')
        res = es.count(index=index, body=body)
        return res


class Create(Resource):
    def put(self, index, id):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        parser = reqparse.RequestParser()
        parser.add_argument("data")
        args = parser.parse_args()
        body = str(args["data"]).replace("'", '"')
        es.create(index=index, body=body, id=id)


class Delete(Resource):
    def delete(self, index, id):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        es.delete(index=index, id=id)


class IndexInfo(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.indices(index=index, format='json')
        return res


class MasterInfo(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.master(format='json')
        return res


class NodeAttrs(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.nodeattrs(format='json')
        return res


class Nodes(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.nodes(format='json')
        return res


class Recovery(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.recovery(index=index, format='json')
        return res


class Segments(Resource):
    def get(self, index):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.segments(index=index, format='json')
        return res


class Templates(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cat.templates(format='json')
        return res


class ClusterAllocationExplain(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cluster.allocation_explain()
        return res


class ClusterSettings(Resource):
    def get(self):
        es = Elasticsearch([{'host': '10.8.173.181', 'port': 80}])
        res = es.cluster.get_settings()
        return res


api.add_resource(Document, "/<string:index>/<string:type>/<string:id>")
api.add_resource(Documents, "/<string:index>/docs")
api.add_resource(DeleteByQuery, "/<string:index>/delete_by_query")
api.add_resource(UpdateByQuery, "/<string:index>/update_by_query")
api.add_resource(Settings, "/<string:index>/settings")
api.add_resource(Mapping, "/<string:index>/mapping")
api.add_resource(Alias, "/<string:index>/alias")
api.add_resource(OpenIndex, "/<string:index>/open")
api.add_resource(CloseIndex, "/<string:index>/close")
api.add_resource(CreateIndex, "/<string:index>")
api.add_resource(ShrinkIndex, "/<string:sourceIndex>/shrink/<string:targetIndex>")
api.add_resource(SplitIndex, "/<string:sourceIndex>/split/<string:targetIndex>")
api.add_resource(Source, "/<string:index>/<string:type>/<string:id>/source")
api.add_resource(Bulk, "/bulk")
api.add_resource(Reindex, "/reindex")
api.add_resource(Aliases, "/aliases")
api.add_resource(IndexHealth, "/<string:index>/health")
api.add_resource(Search, "/<string:index>/search")
api.add_resource(Count, "/<string:index>/count")
api.add_resource(Aggregation, "/<string:index>/aggs")
api.add_resource(Create, "/<string:index>/create/<string:id>")
api.add_resource(Delete, "/<string:index>/delete/<string:id>")
api.add_resource(Mappings, "/mapping")
api.add_resource(ClusterStats, "/cluster/stats")
api.add_resource(ClusterAllocationExplain, "/cluster/allocation/explain")
api.add_resource(ClusterSettings, "/cluster/settings")
api.add_resource(ClusterHealth, "/cluster/health")
api.add_resource(AllAlias, "/cat/alias")
api.add_resource(Allocation, "/cat/allocation")
api.add_resource(AllocationNode, "/cat/allocation/<string:name>")
api.add_resource(IndexInfo, "/cat/<string:index>")
api.add_resource(MasterInfo, "/cat/master")
api.add_resource(NodeAttrs, "/cat/nodeattrs")
api.add_resource(Nodes, "/cat/nodes")
api.add_resource(Recovery, "/cat/recovery/<string:index>")
api.add_resource(Segments, "/cat/segments/<string:index>")
api.add_resource(Templates, "/cat/templates")
api.add_resource(NodesStats, "/node/stats")
api.add_resource(NodeStats, "/node/<string:id>/stats")
api.add_resource(Indices, "/indices")

app.run(debug=True)