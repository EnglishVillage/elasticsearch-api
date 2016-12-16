package elasticsearch.api;

import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.sort.SortBuilder;
import solutions.siren.join.action.coordinate.CoordinateSearchRequestBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by cube on 16-11-23.
 */
public class ESUtils {


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:24
     * 名称：获取index下type的mapping信息
     * 备注：
     */
    public static String getMapping(String index, String type) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().get()
                    .getState().getMetaData().getIndices().get(index).getMappings();
            String s = mappings.get(type).source().toString();
            return s;
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:54
     * 名称：新增/修改的时候，refresh一下可以马上查询到。
     * 备注：每次新增/修改使用对性能有很大的影响
     */
    public static boolean refresh(String... index) {
        Client client = ESClientHelper.getInstance().getClient();
        RefreshResponse response = client.admin().indices().prepareRefresh().get();
        return response.isContextEmpty();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午7:55
     * 名称：判断indexes是否存在
     * 备注：
     */
    public static boolean isExistIndex(String... indexes) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(indexes)).get();
            return indicesExistsResponse.isExists();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午7:55
     * 名称：判断typees是否存在
     * 备注：
     */
    public static boolean isExistType(String[] indexes, String[] type) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            TypesExistsResponse typeResponse = client.admin().indices().prepareTypesExists(indexes).setTypes(type).get();
            return typeResponse.isExists();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 上午10:12
     * 名称：创建空索引 默认setting 无mapping
     * 备注：5个分片，1个备份
     * 索引存在会抛出异常：IndexAlreadyExistsException
     */
    public static boolean createIndex(String index) {
        try {
            Client client = ESClientHelper.getInstance().getClient();
            CreateIndexResponse response = client.admin().indices().prepareCreate(index).get();
            return response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 上午10:12
     * 名称：创建空索引 默认setting 无mapping
     * 备注：索引存在会抛出异常：IndexAlreadyExistsException
     * shardNum:分片数（默认5）
     * replicaNum：备份数（默认1）
     */
    public static boolean createIndex(String index, int shardNum, int replicaNum) {
        try {
            Client client = ESClientHelper.getInstance().getClient();
            CreateIndexResponse response = client.admin().indices().prepareCreate(index)
                    .setSettings(Settings.builder().put("index.number_of_shards", shardNum).put("index.number_of_replicas", replicaNum).build())
                    .get();
            return response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:46
     * 名称：给
     * 备注：如果index中已经有相同的字段，但是这次修改mapping跟上次类型不一致，则会报如下异常
     * java.lang.IllegalArgumentException:
     * mapper [name] of different type, current_type [string], merged_type [long]
     */
    public static boolean createType(String index, String type, XContentBuilder mapping) {
        Client client = ESClientHelper.getInstance().getClient();
        PutMappingResponse response = client.admin().indices()
                .preparePutMapping(index)
                .setType(type)
                .setSource(mapping)
                .get();
        return response.isAcknowledged();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */
    public static boolean operDelete(String index, String type, String id) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            if (id != null) {
                DeleteResponse response = client.prepareDelete(index, type, id)
                        .get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
                System.out.println(response.getIndex() + "-" + response.getType() + "-" + response.getId() + "[" + response.getVersion() + "]" + ":" + response.isFound());
                return response.isFound();
            } else {
                if (type != null) {
                    if (index != null) {
                        //先判断type是否存在
                        TypesExistsResponse typeResponse = client.admin().indices().prepareTypesExists(index).setTypes(type).get();
                        if (typeResponse.isExists()) {
                            DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                                    .setIndices(index).setTypes(type)
                                    .setSource("{\"query\": {\"match_all\": {}}}")
                                    .get();
                            return response.isContextEmpty();
                        }
                    }
                } else {
                    if (index != null) {
                        //先判断index是否存在
                        IndicesAdminClient indices = client.admin().indices();
                        IndicesExistsResponse indicesExistsResponse = indices.exists(new IndicesExistsRequest(index)).get();
                        if (indicesExistsResponse.isExists()) {
                            DeleteIndexResponse deleteIndexResponse = indices.prepareDelete(index).get();
                            return deleteIndexResponse.isAcknowledged();
                        }
                    }
                }
                return false;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：软删除
     * 备注：
     */
    public static boolean operDeleteSoft(String index, String type, String id) {
        //new Script("ctx._source.gender = \"male\"")   格式：【ctx.source.属性名=XXXX】
        return operUpdate(index, type, id, new Script("ctx._source.isDelete = true"));
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:33
     * 名称：添加/修改
     * 备注：
     * index:数据库
     * type：表
     * id:主键【不指定则自动生成，自动生成则无法修改】
     * version:版本号，修改的时候加上version，避免覆盖掉新的数据。.【如果小于1则不设置版本号】
     * source：数据源【不可以为空，对象个数至少一个或者为偶数个。】
     */
    public static boolean operIndex(String index, String type, String id, Object source) {
        if (source == null) {
            System.err.println("插入数据不可以为可空！");
            return false;
        } else {
            //转化为json字符串
            String jsonArr = ObjectToJson(source);
            Client client = ESClientHelper.getInstance().getClient();
            //初始化Request，传入index和type参数
            IndexRequestBuilder request = client.prepareIndex(index, type);
            //id不为空,传入id参数;id为空，则自动生成
            if (id != null) {
                request = request.setId(id);//必须为对象单独指定ID,不指定则自动生成
            }
            request = request.setSource(jsonArr);
            //执行操作，并返回操作结果,底层调用//.execute().actionGet();
            IndexResponse response = request.get();
            //多次index这个版本号会变
            //System.out.println("index response.version():" + response.getVersion());
            //response.isCreated();//只返回新增是否成功
            //返回新增或者修改是否成功
            return response.isContextEmpty();
        }
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:33
     * 名称：添加/修改
     * 备注：
     * index:数据库
     * type：表
     * id:主键【不指定则自动生成，自动生成则无法修改】
     * version:版本号，修改的时候加上version，避免覆盖掉新的数据。.【如果小于1则不设置版本号】
     * source：数据源【不可以为空，对象个数至少一个或者为偶数个。】
     */
    public static boolean operIndex(String index, String type, String id, long currVersion, String parent, Object source) {
        if (source == null) {
            System.err.println("插入数据不可以为可空！");
            return false;
        } else {
            //转化为json字符串
            String jsonArr = ObjectToJson(source);
            Client client = ESClientHelper.getInstance().getClient();
            //初始化Request，传入index和type参数
            IndexRequestBuilder request = client.prepareIndex(index, type);
            //传入id参数
            if (id != null) {
                request = request.setId(id);//必须为对象单独指定ID,不指定则自动生成
            } else {
                //id为空，则自动生成
            }
            //传入parent参数
            if (parent != null) {
                request = request.setParent(parent);
            } else {
                //parent为空，则自动生成
            }
            //传入当前要操作的文档的版本号，一定要对应，不然修改失败。
            if (currVersion > 0) {
                request = request.setVersion(currVersion);
            }
            request = request.setSource(jsonArr);
            try {
                //执行操作，并返回操作结果,底层调用//.execute().actionGet();
                IndexResponse response = request.get();
                //多次index这个版本号会变
                //System.out.println("index response.version():" + response.getVersion());
                //response.isCreated();//只返回新增是否成功
                //返回新增或者修改是否成功
                return response.isContextEmpty();
            } catch (VersionConflictEngineException ex) {
                System.err.println("修改操作，请确定当前要更新的文档的版本号。添加操作，请将版本号设置为0。");
                return false;
            }
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键
     */
    public static String operGet(String index, String type, String id) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            GetResponse response = client.prepareGet(index, type, id).get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
            return response.getSourceAsString();
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键
     */
    public static <T extends Object> T operGet(String index, String type, String id, Class<T> clazz) {
        Client client = ESClientHelper.getInstance().getClient();
        GetResponse response = client.prepareGet(index, type, id).get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        String json = response.getSourceAsString();
        return gson.fromJson(json, clazz);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个(值是精确匹配,不是模糊匹配,且isDelete=false)
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * field:要查询字段
     * value:字段值
     * clazz:要转化的Class对象
     */
    public static <T extends Object> T operGetByFieldWithNoDel(String index, String type, String field, Object value, Class<T> clazz) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery(field, value));
        return operGetByQueryWithNoDel(index, type, query, clazz);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个(值是精确匹配,不是模糊匹配)
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * field:要查询字段
     * value:字段值
     * clazz:要转化的Class对象
     */
    public static <T extends Object> T operGetByField(String index, String type, String field, Object value, Class<T> clazz) {
        QueryBuilder query = QueryBuilders.matchPhraseQuery(field, value);
        return operGetByQuery(index, type, query, clazz);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个(值是精确匹配,不是模糊匹配,且isDelete=false)
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * field:要查询字段
     * value:字段值
     * clazz:要转化的Class对象
     */
    public static <T extends Object> T operGetByQueryWithNoDel(String index, String type, BoolQueryBuilder query, Class<T> clazz) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = client.prepareSearch(index, type);
        if (query == null) {
            query = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("isDelete", false));
        } else {
            query = query.must(QueryBuilders.matchQuery("isDelete", false));
        }
        request = request.setQuery(query);
        SearchResponse response = request.setSize(1).get();
        for (SearchHit hit : response.getHits()) {
            String json = hit.getSourceAsString();
            Gson gson = new Gson();
            return gson.fromJson(json, clazz);
        }
        return null;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个(值是精确匹配,不是模糊匹配,且isDelete=false)
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * field:要查询字段
     * value:字段值
     * clazz:要转化的Class对象
     */
    public static <T extends Object> T operGetByQuery(String index, String type, QueryBuilder query, Class<T> clazz) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = client.prepareSearch(index, type);
        if (query != null) {
            request = request.setQuery(query);
        }
        SearchResponse response = request.setSize(1).get();
        for (SearchHit hit : response.getHits()) {
            String json = hit.getSourceAsString();
            Gson gson = new Gson();
            return gson.fromJson(json, clazz);
        }
        return null;
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：根据id数组获取多个
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * ids:主键
     */
    public static List<Map<String, Object>> operGetMulti(String index, String type, String... ids) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            MultiGetResponse responses = client.prepareMultiGet()
                    .add(index, type, ids)
                    .get();
            return printGetResponses(responses);
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：修改文档或新增字段
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键，不存在不会报错
     * fieldValue:field存在则修改字段值，field不存在则添加该字段和值
     */
    public static boolean operUpdate(String index, String type, String id, Map<String, Object> fieldValue) {
        if (fieldValue == null || fieldValue.size() < 1) {
            System.err.println("要修改的Map不可以为空！");
            return false;
        }
        Client client = ESClientHelper.getInstance().getClient();
        try {
            //创建要修改的doc文档
            XContentBuilder doc = jsonBuilder().startObject();
            for (String key : fieldValue.keySet()) {
                doc = doc.field(key, fieldValue.get(key));
            }
            doc.endObject();

//            //只能修改不能添加（添加会报错）
////          第1种方式
//            UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(doc).get();
//            第2种方式
//            UpdateRequest request = new UpdateRequest(index, type, id);
//            request = request.doc(request);
//            UpdateResponse response = client.update(request).get();


            //可修改亦可添加（和client.prepareIndex(...)基本类似，但是感觉很鸡肋。）
            IndexRequest request = new IndexRequest(index, type, id);
            //封装成一个update请求
            request = request.source(doc);
            //创建insert请求
            UpdateRequest request2 = new UpdateRequest(index, type, id);
            //封装成一个insert请求
            String firstKey = fieldValue.keySet().toArray(new String[]{})[0];
            request2 = request2.doc(jsonBuilder().startObject().field(firstKey, fieldValue.get(firstKey)).endObject()).upsert(request);
            UpdateResponse response = client.update(request2).get();

            return response.isContextEmpty();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：更新【这个update方法柑橘不是那么好用，直接用operIndex操作】
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键，不存在不会报错
     * script:new Script("ctx._source.gender = \"male\"")   格式：【ctx.source.属性名=XXXX】
     * 里面的语法是groovy写的。
     */
    public static boolean operUpdate(String index, String type, String id, Script script) {
        Client client = ESClientHelper.getInstance().getClient();
        UpdateRequest request = new UpdateRequest(index, type, id);
        try {
            request = request.script(script);
            UpdateResponse response = client.update(request).get();
            return response.isContextEmpty();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午6:46
     * 名称：批处理添加修改(不知道_id)
     * 备注：
     * jsons:json字符串集合
     */
    public static boolean operBulk(String index, String type, List<String> jsons) {
        Client client = ESClientHelper.getInstance().getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String json : jsons) {
            bulkRequest.add(client.prepareIndex(index, type).setSource(json));
        }
        BulkResponse response = bulkRequest.get();
        return response.isContextEmpty();
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/15 10:47
     * 名称：批处理添加修改(知道_id)
     * 备注：
     * jsons:json字符串集合
     * ids:id集合
     * 2个集合总元素数量要相等
     */
    public static boolean operBulk(String index, String type, List<String> jsons, List<String> ids) {
        Client client = ESClientHelper.getInstance().getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 0; i < jsons.size(); i++) {
            bulkRequest.add(client.prepareIndex(index, type, ids.get(i)).setSource(jsons.get(i)));
        }
        BulkResponse response = bulkRequest.get();
        return response.isContextEmpty();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     * pageIndex:页数[默认为1]
     * pageSize:页码[默认为10]
     */
    public static SearchResponse operSearchWithNoDel(String index, String type, BoolQueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, index, type, query, filter);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //后续查找
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     * pageIndex:页数[默认为1]
     * pageSize:页码[默认为10]
     */
    public static SearchResponse operSearch(String index, String type, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, index, type, query, filter);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //后续查找
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     * pageIndex:页数[默认为1]
     * pageSize:页码[默认为10]
     */
    public static SearchResponse operSearchWithNoDel(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();

        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //后续查找
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询(不分页)
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     */
    public static SearchResponse operSearchWithNoDel(String index, String type, BoolQueryBuilder query, QueryBuilder filter, SortBuilder[] sorts) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, index, type, query, filter);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //后续查找
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request.setSize(MAX_SIZE);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询(不分页)
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     */
    public static SearchResponse operSearch(String index, String type, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, index, type, query, filter);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //后续查找
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request.setSize(MAX_SIZE);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchAggWithNoDel(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
            //TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
            //DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchAggWithNoDel(String index, String type, BoolQueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, index, type, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
            //TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
            //DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchAgg(String index, String type, BoolQueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, index, type, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
            //TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
            //DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    private static void operSearchAggWithNoDel(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders, String[] aggNames) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
//            TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
//            DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        // Get your facet results
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            Map<String, Object> metaData = aggregation.getMetaData();
//            DateHistogram agg2 = sr.getAggregations().get("agg2");


//            //常用聚合函数
//            Min agg = response.getAggregations().get(aggNames[i]);
//            Max agg = response.getAggregations().get(aggNames[i]);
//            Sum agg = response.getAggregations().get(aggNames[i]);
//            Avg agg = response.getAggregations().get(aggNames[i]);
//            ValueCount agg = response.getAggregations().get(aggNames[i]);
//            //其他
//            Cardinality agg=(Cardinality)aggregation;     //基数
//            System.out.println(agg.getName()+":"+agg.getValue());
//            //常用聚合函数
//            Stats stats = response.getAggregations().get(aggNames[i]);
//            System.out.println(stats.getName()+
//                    ":\tmin:"+stats.getMinAsString()+
//                    "\tmax:"+stats.getMaxAsString()+
//                    "\tsum:"+stats.getSumAsString()+
//                    "\tavg:"+stats.getAvgAsString()+
//                    "\tcount:"+stats.getCountAsString());
//            //常用聚合函数+标准差，平方和，方差
//            ExtendedStats extendedStats = response.getAggregations().get(aggNames[i]);
//            System.out.println(extendedStats.getName()+
//                    ":\tmin:"+extendedStats.getMinAsString()+
//                    "\tmax:"+extendedStats.getMaxAsString()+
//                    "\tsum:"+extendedStats.getSumAsString()+
//                    "\tavg:"+extendedStats.getAvgAsString()+
//                    "\tcount:"+extendedStats.getCountAsString()+
//                    "\tstdDeviation:"+extendedStats.getStdDeviationAsString()+  //标准差
//                    "\tsumOfSquares:"+extendedStats.getSumOfSquaresAsString()+  //平方和
//                    "\tvariance:"+extendedStats.getVarianceAsString());         //方差
//            //抽取数据进行判断数据的一些东西。 参考：http://blog.csdn.net/asia_kobe/article/details/50170937
//            Percentiles pers=(Percentiles)aggregation;
//            for(Percentile per:pers){
//                //随机抽取百分多少的数据：XXX
//                System.out.println(per.getPercent()+":"+per.getValue());
//            }
//            //参考：https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html
//            PercentileRanks persRanks=(PercentileRanks)aggregation;
//            for(Percentile per:persRanks){
//                //随机抽取百分多少的数据：XXX
//                System.out.println(per.getPercent()+":"+per.getValue());
//            }
//            //获取边界【经纬度】参考：https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-geobounds-aggregation.html
//            GeoBounds geoBounds=(GeoBounds)aggregation;
//            System.out.println("bottomRight:"+geoBounds.bottomRight()+"\ttopLeft:"+geoBounds.topLeft());
//            //分组取几条数据（默认是3条）
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                    // bucket key
//                long docCount = entry.getDocCount();            // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                // We ask for top_hits for each bucket
//                TopHits topHits = entry.getAggregations().get("top");
//                for (SearchHit hit : topHits.getHits().getHits()) {
//                    System.out.println(" -> id:" + hit.getId() + ", _source" + hit.getSourceAsString());
//                }
//            }
//            //分组求元素个数总数,平均,和,最大值,最小值
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();
//                long docCount = entry.getDocCount();
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                Avg chiHit = entry.getAggregations().get("avg");
//                Sum chiHit2 = entry.getAggregations().get("sum");
//                System.out.println("avg:" + chiHit.getValue() + "\tsum：" + chiHit2.getValue());
//            }


//            //********************************************************************
//            //父子表中：先根据父表的country分组，在设置子表，在根据子表的hobby进行分组。
//            //********************************************************************
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKeyAsString();
//                long docCount = entry.getDocCount();
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                Children children = entry.getAggregations().get("employee");
//                Terms chiTerms=children.getAggregations().get("hobby");
//                for (Terms.Bucket chiEntry : chiTerms.getBuckets()) {
//                    key=chiEntry.getKeyAsString();
//                    docCount = chiEntry.getDocCount();
//                    System.out.println("\t--> key:" + key + ", doc_count:" + docCount);
//                }
//            }


        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：根据pageSize分批次查询
     * 备注：
     * index:数据库
     * type：表
     * id:主键
     * time:获取之后存储的时间，单位毫秒
     * pageSize:每次滚动显示的条数
     */
    public static List<SearchResponse> operSearchScroll(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int time, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        request = getRequestBySort(request, sorts);
        //获取之后存储的时间，单位毫秒
        TimeValue tValue;
        if (time > 0) {
            tValue = new TimeValue(time);
        } else {
            tValue = new TimeValue(3000);
        }
        request = request.setScroll(tValue);
        //每次滚动显示的条数
        if (pageSize > 0) {
            request = request.setSize(pageSize);
        } else {
            request = request.setSize(10);
        }
        SearchResponse scrollResp = request.get();
        ArrayList<SearchResponse> list = new ArrayList<>();
        for (int i = 1; ; i++) {
            list.add(scrollResp);
            System.out.println("第" + i + "次滚动：");
            printSearchHits(scrollResp);
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tValue).get();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        return list;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static long operSearchCountWithNoDel(String index, String type, BoolQueryBuilder query, QueryBuilder filter) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, index, type, query, filter);
        request = request.setSize(0);//文档里面说的把size设置为0
        SearchResponse response = request.get();
        return response.getHits().getTotalHits();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static long operSearchCount(String index, String type, QueryBuilder query, QueryBuilder filter) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, index, type, query, filter);
        request = request.setSize(0);//文档里面说的把size设置为0
        SearchResponse response = request.get();
        return response.getHits().getTotalHits();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static long operSearchCountWithNoDel(String[] indexes, String[] types, BoolQueryBuilder boolQuery, QueryBuilder filter) {
        Client client = ESClientHelper.getInstance().getClient();
        QueryBuilder query = boolQuery.must(QueryBuilders.matchQuery("isDelete", false));
        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        request = request.setSize(0);//文档里面说的把size设置为0
        SearchResponse response = request.get();
        return response.getHits().getTotalHits();
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/5 19:26
     * 名称：关联查询【方法和DeleteByQuery插件类似】
     * 备注：
     */
    public static SearchResponse operSearchSiren(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        //DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
        SearchRequestBuilder request = new CoordinateSearchRequestBuilder(client);//根据上面方式,查找package包写的.
        //SearchRequestBuilder request = CoordinateSearchAction.INSTANCE.newRequestBuilder(client);//和上面是等价的
        request = request.setIndices(indexes);
        request = request.setTypes(types);
        if (query != null) {
            request = request.setQuery(query);
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        printSearchHits(response);//输出response结果
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取【还要优化】
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static List<SearchResponse> operSearchMulti(String[] indexes, String[] types, QueryBuilder[] querys, QueryBuilder filter, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        MultiSearchRequestBuilder request = client.prepareMultiSearch();
        //request添加多个子request
        for (int i = 0; i < querys.length; i++) {
            //子的Request
            SearchRequestBuilder chiRequest = getRequestWithNoDel(client, indexes, types, querys[i], filter);
            request = request.add(chiRequest);
        }
        MultiSearchResponse multiResponse = request.get();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long sumHits = 0;
        float sumSeconds = 0f;
        ArrayList<SearchResponse> list = new ArrayList<SearchResponse>();
        for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
            SearchResponse response = item.getResponse();
            list.add(response);
            sumHits += response.getHits().getTotalHits();
            sumSeconds += response.getTookInMillis() / 1000f;
        }
        System.out.println("各个条件总命中数：" + sumHits + "\t总时间（单位s）：" + sumSeconds);
        return list;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询【20个数据，查询（小于等于6）个才有效。30%以下才有效果？？？】
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchTerminate(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, int maxNum) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequestWithNoDel(client, indexes, types, query, filter);
        request = request.setSize(maxNum);
        //设置获取多少条数据就停止搜索
        request = request.setTerminateAfter(maxNum);

        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //判断是否提前完成。
        if (response.isTerminatedEarly()) {
            // We finished early
            System.out.println("We finished early");
        }
        printSearchHits(response);
        return response;
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:59
     * 名称：将对象转化为json字符串
     * 备注：
     */
    public static String ObjectToJson(Object o) {
        //第0种方法
        return gson.toJson(o);
        //其他方法参考[版本]：
        // http://blog.csdn.net/napoay/article/details/51707023
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-docs-index.html

        //ObjectMapper mapper = new ObjectMapper();
        //byte[] json = mapper.writeValueAsBytes(obj);
    }

    private static Map<String, Object> printGetResponse(GetResponse response) {
        if (response.isExists()) {
            System.out.println("response.getId():" + response.getId() + ":" + response.isExists());
            System.out.println(response.getId() + ":" + response.getSourceAsString() + "\tversion:" + response.getVersion());
            return response.getSourceAsMap();
        } else {
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-17 下午7:33
     * 名称：打印输出SearchResponse所有元素
     * 备注：
     */
    private static List<Map<String, Object>> printGetResponses(MultiGetResponse responses) {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        GetResponse response;
        Map<String, Object> map;
        for (MultiGetItemResponse itemResponse : responses) {
            response = itemResponse.getResponse();
            map = printGetResponse(response);
            if (map != null) {
                list.add(map);
            }
        }
        return list;
    }

    private static SearchRequestBuilder getRequestWithNoDel(Client client, String index, String type, BoolQueryBuilder query, QueryBuilder filter) {
        SearchRequestBuilder request = client.prepareSearch(index);
        if (type != null && type.length() > 0) {
            request = request.setTypes(type);
        }
        if (query != null) {
            query = query.must(QueryBuilders.matchQuery("isDelete", false));
            request = request.setQuery(query);
        } else {
            request = request.setQuery(QueryBuilders.matchQuery("isDelete", false));
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        return request;
    }

    private static SearchRequestBuilder getRequest(Client client, String index, String type, QueryBuilder query, QueryBuilder filter) {
        SearchRequestBuilder request = client.prepareSearch(index);
        if (type != null && type.length() > 0) {
            request = request.setTypes(type);
        }
        if (query != null) {
            request = request.setQuery(query);
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        return request;
    }

    private static SearchRequestBuilder getRequestWithNoDel(Client client, String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter) {
        SearchRequestBuilder request = client.prepareSearch(indexes);
        if (types != null && types.length > 0) {
            request = request.setTypes(types);
        }
        if (query != null) {
            request = request.setQuery(query);
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        return request;
    }

    private static SearchRequestBuilder getRequestByFields(SearchRequestBuilder request, String[] fields) {
        //添加要显示的字段
        if (fields != null && fields.length > 0) {
            request = request.addFields(fields);
        }
        return request;
    }

    private static SearchRequestBuilder getRequestBySort(SearchRequestBuilder request, SortBuilder[] sorts) {
        //添加排序
        if (sorts != null && sorts.length > 0) {
            //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
            for (SortBuilder sort : sorts) {
                request = request.addSort(sort);
            }
        }
        return request;
    }

    private static SearchRequestBuilder getRequestByPage(SearchRequestBuilder request, int pageIndex, int pageSize) {
        //分页
        if (pageIndex > 0 || pageSize > 0) {
            pageIndex = pageIndex > 0 ? pageIndex : 1;
            pageSize = pageSize > 0 ? pageSize : 10;
            request = request.setFrom((pageIndex - 1) * pageSize).setSize(pageSize);
        } else {
            //不分页【默认显示10条】
            //request=request.setSize(10);
        }
        return request;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-17 下午7:33
     * 名称：打印输出SearchResponse所有元素
     * 备注：
     */
    public static void printSearchHits(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("命中数：" + hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + ":" + hit.getSourceAsString() + "\tscore:" + hit.getScore());
        }
    }


    //分页查询最大的条数
    public static final int MAX_SIZE = 10000;
    private static final Gson gson = new Gson();

}



