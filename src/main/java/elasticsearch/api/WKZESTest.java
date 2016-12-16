package elasticsearch.api;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static solutions.siren.join.index.query.QueryBuilders.filterJoin;

/**
 * 名称：王坤造
 * 时间：2016/12/15.
 * 名称：
 * 备注：
 */
public class WKZESTest {
    String index = "testcreateindex";
    String type = "testtype";
    String id = null;

    @Test
    public void mytest() throws Exception {
        index = "newkangkang";
        type = "newdata";
//        QueryBuilder query = QueryBuilders.matchAllQuery();
//        QueryBuilder filter = null;
//        SortBuilder[] sorts = {
//                //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
//                //SortBuilders.fieldSort("transxm").order(SortOrder.DESC),
//                SortBuilders.fieldSort("custid").order(SortOrder.ASC)
//        };
        //MyWork.ESUtils.operDelete("discoverdrugs2","discoverdrugs",null);


        index = "discoverdrugssalse";
        type = "discoverdrugssalse";

//        HashMap<String, Object> map = new HashMap<>();
//        String codeName = "companyCode";
//        String nameName = "companyName";
//        List<String> codeList = new ArrayList<>();
//        List<String> nameList = new ArrayList<>();
//        SearchResponse response = ESUtils.operSearchWithNoDel(index, type, null, null, null);
//        for (SearchHit hit : response.getHits()) {
//            Map<String, Object> stringObjectMap = hit.sourceAsMap();
//            String codeValue = (String) stringObjectMap.get(codeName);
//            String nameValue = (String) stringObjectMap.get(nameName);
//            if (!StringUtils.isEmpty(codeValue) && !StringUtils.isEmpty(nameValue)) {
//                codeList.add(codeValue);
//                nameList.add(nameValue);
//            }
//        }
//        System.out.println(codeList.size());


        //BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.fuzzyQuery("code","M000075D01"));
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.wildcardQuery("code","*M003808D01*"));
        SearchResponse searchResponse = ESUtils.operSearch(index, type, query, null, null);
        System.out.println(searchResponse.getHits().getTotalHits());

        System.out.println();
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:35
     * 名称：创建/判断/删除index
     * 备注：
     */
    @Test
    public void testcreateIndex() {
        System.out.println("创建空index'" + index + "':" + ESUtils.createIndex(index, 3, 3));
        System.out.println("空index'" + index + "'存在：" + ESUtils.isExistIndex(index));
        System.out.println("空index'" + index + "'删除成功：" + ESUtils.operDelete(index, null, null));
        System.out.println("创建空index'testcreateindex':" + ESUtils.createIndex(index));
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:37
     * 名称：给index添加mapping【不推荐，建议使用插件或者DSL方式修改】
     * 备注：
     */
    @Test
    public void testopercreateType() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("properties")
                .startObject("name").field("type", "string").field("store", "yes").endObject()
                .startObject("age").field("type", "long").field("store", "yes").endObject()
                .startObject("birthday").field("type", "date").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject();

        boolean b = ESUtils.createType(index, type, mappingBuilder);
        System.out.println("给index'" + index + "'添加mapping:" + b);
        System.out.println(ESUtils.getMapping(index, type));

        try {
            mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(type)
                    .startObject("properties")
                    .startObject("name").field("type", "string").field("store", "yes").endObject()
                    .startObject("age").field("type", "long").field("store", "no").endObject()
                    .startObject("birthday").field("type", "date").field("store", "yes").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            b = ESUtils.createType(index, type, mappingBuilder);
            System.out.println("给index'" + index + "'添加mapping:" + b);
        } catch (Exception e) {
            System.err.println("如果index中已经有相同的字段，但是这次修改mapping跟上次类型不一致，则会报如下异常");
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:11
     * 名称：增删改操作
     * 备注：
     */
    @Test
    public void testoperCRUD() {
        String id = "1";
        String id2 = "2";
        String id3 = "3";
        String parent = null;
        Object source = new Person("aaa", 111, "2016-01-01", new ArrayList<Book>() {{
            add(new Book("storm", 919));
        }});
        Object source2 = new Person("bbb", 222, "2016-03-23T19:32:43", new ArrayList<Book>() {{
            add(new Book("hadoop", 919));
        }});


        ESUtils.operIndex(index, type, id, 0, parent, source);//新增：自己指定id
        ESUtils.operIndex(index, type, null, 0, parent, source2);//新增：id自动生产
        ESUtils.operIndex(index, type, id, 0, parent, source2);//修改：必须确定id
        ESUtils.operIndex(index, type, id2, 0, parent, source2);//修改：必须确定id
        ESUtils.operIndex(index, type, id3, 0, parent, source);//修改：必须确定id
        ESUtils.operGet(index, type, id3);
        List<Map<String, Object>> list = ESUtils.operGetMulti(index, type, id, id2, id3);
        System.out.println("id:'" + id3 + "'删除成功：" + ESUtils.operDelete(index, type, id3));
        //1.修改字段值
        ESUtils.operGet(index, type, id2);
        HashMap<String, Object> updateMap = new HashMap<String, Object>() {{
            put("birthday", "2015-03-23T19:32:43");
        }};
        System.out.println("1.修改字段值");
        ESUtils.operUpdate(index, type, id, updateMap);
        ESUtils.operGet(index, type, id2);
        //1.如果字段不存在，则添加字段
        updateMap.put("newField", "newValue");
        System.out.println("1.如果字段不存在，则添加字段");
        ESUtils.operUpdate(index, type, id2, updateMap);
        ESUtils.operGet(index, type, id2);
        //2.使用脚本方式：修改字段值
        //new Script("ctx._source.gender = \"male\"")   格式：【ctx.source.属性名=XXXX】
        System.out.println("2.使用脚本方式：修改字段值");
        ESUtils.operUpdate(index, type, id2, new Script("ctx._source.birthday = \"2015-08-23T19:32:43\""));
        ESUtils.operGet(index, type, id2);
        //2.使用脚本方式：如果字段不存在，则添加字段
        System.out.println("2.如果字段不存在，则添加字段");
        ESUtils.operUpdate(index, type, id2, new Script("ctx._source.gender = \"male\""));
        ESUtils.operGet(index, type, id2);
        //2.删除这个id的字段，并不会删除其他id的字段
        System.out.println("2.删除这个id的字段，并不会删除其他id的字段");
        ESUtils.operUpdate(index, type, id2, new Script("ctx._source.remove(\"gender\")"));
        ESUtils.operGet(index, type, id2);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-24 上午11:32
     * 名称：查询案例1
     * 备注：
     */
    @Test
    public void testSearch1() {
        index = "newkangkang";
        type = "newdata";

        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        SortBuilder[] sorts = {
                //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
                //SortBuilders.fieldSort("transxm").order(SortOrder.DESC),
                SortBuilders.fieldSort("custid").order(SortOrder.ASC)
        };

//        //批处理导入数据
//        MyWork.ESUtils.operBulk(index, type, Arrays.asList(getJsonArr()));
//        //新增修改数据，有1s延迟，刷新会马上查询到
//        System.out.println("刷新成功：" + MyWork.ESUtils.refresh(index));
//        SearchResponse response = MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//
//        System.out.println(MyWork.ESUtils.getMapping(index, type));
//        //字段查询
//        //filter= QueryBuilders.termQuery("merchants", "aaa");//no   【string默认有分词】
//        query = QueryBuilders.termQuery("merchants", "Harvey Nichols");//no
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("merchants", "Harvey Nichols");//yes
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.termQuery("merchants", "chols");//no   【string默认有分词】
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("merchants", "chols");//no     【根据空格分词】
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.termQuery("atmFrequency", 10);     //yes
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("atmFrequency", 10);     //yes
//        MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);

        //范围查询【左右都闭合】
        query = QueryBuilders.rangeQuery("custid").from(10011).to(10015);   //yes
        String s = query.toString();
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
        query = QueryBuilders.rangeQuery("birthday").from("2015-03-23").to("2016-03-23T19:32:43").includeUpper(true).includeLower(true);   //yes
        ESUtils.operSearchWithNoDel(new String[]{"testcreateindex"}, new String[]{"testtype"}, query, filter, null, 0, 20);


        //批次查询
//        QueryBuilder[] querys = {
//                QueryBuilders.rangeQuery("custid").from(10011).to(10015),
//                QueryBuilders.termQuery("atmFrequency", 10),
//                QueryBuilders.termQuery("transxm",17)
//        };
//        MyWork.ESUtils.operSearchMulti(new String[]{index}, null, querys, filter, 0, 0);
//        MyWork.ESUtils.operSearchScroll(new String[]{index}, new String[]{type}, query, filter, sort,30000,15);
//        MyWork.ESUtils.operSearchTerminate(new String[]{index}, new String[]{type}, query, filter,7);//20条数据，<7才有效果
    }

    private String[] getJsonArr() {
        String[] jsonArr =
                {
                        "{\"custid\":10007,\"ntranstime\":1475437449785,\"transamt\":10222.14,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10222.14,\"atmTime\":1475437449785,\"atmAmount\":3923.15,\"atmFrequency\":6,\"atmCurrentPersonAmt\":3923.15}",
                        "{\"custid\":10010,\"ntranstime\":1475437446779,\"transamt\":8305.7705,\"merchants\":\"Carrefour\",\"transxm\":10,\"CurrentPersonAmt\":8305.7705,\"atmTime\":1475437416723,\"atmAmount\":5747.5996,\"atmFrequency\":6,\"atmCurrentPersonAmt\":5747.5996}",
                        "{\"custid\":10001,\"ntranstime\":1475437442772,\"transamt\":3490.9697,\"merchants\":\"La Rlnascente\",\"transxm\":5,\"CurrentPersonAmt\":3490.9697,\"atmTime\":1475437438764,\"atmAmount\":6408.8696,\"atmFrequency\":10,\"atmCurrentPersonAmt\":6408.8696}",
                        "{\"custid\":10003,\"ntranstime\":1475437361618,\"transamt\":8757.19,\"merchants\":\"Carrefour\",\"transxm\":14,\"CurrentPersonAmt\":8757.19,\"atmTime\":1475437411714,\"atmAmount\":11501.74,\"atmFrequency\":15,\"atmCurrentPersonAmt\":11501.74}",
                        "{\"custid\":10018,\"ntranstime\":1475437435758,\"transamt\":10456.45,\"merchants\":\"Lafayette\",\"transxm\":15,\"CurrentPersonAmt\":10456.45,\"atmTime\":1475437360616,\"atmAmount\":6614.98,\"atmFrequency\":13,\"atmCurrentPersonAmt\":6614.98}",
                        "{\"custid\":10016,\"ntranstime\":1475437424737,\"transamt\":13565.02,\"merchants\":\"Macy's\",\"transxm\":17,\"CurrentPersonAmt\":13565.02,\"atmTime\":1475437447781,\"atmAmount\":9488.27,\"atmFrequency\":13,\"atmCurrentPersonAmt\":9488.27}",
                        "{\"custid\":10009,\"ntranstime\":1475437420730,\"transamt\":4988.57,\"merchants\":\"Macy's\",\"transxm\":9,\"CurrentPersonAmt\":4988.57,\"atmTime\":1475437431750,\"atmAmount\":7295.47,\"atmFrequency\":9,\"atmCurrentPersonAmt\":7295.47}",
                        "{\"custid\":10005,\"ntranstime\":1475437396684,\"transamt\":13259.92,\"merchants\":\"Harrods\",\"transxm\":17,\"CurrentPersonAmt\":13259.92,\"atmTime\":1475437452791,\"atmAmount\":12974.68,\"atmFrequency\":16,\"atmCurrentPersonAmt\":12974.68}",
                        "{\"custid\":10012,\"ntranstime\":1475437440768,\"transamt\":8120.5903,\"merchants\":\"Macy's\",\"transxm\":10,\"CurrentPersonAmt\":8120.5903,\"atmTime\":1475437374643,\"atmAmount\":11250.45,\"atmFrequency\":13,\"atmCurrentPersonAmt\":11250.45}",
                        "{\"custid\":10014,\"ntranstime\":1475437410712,\"transamt\":10662.631,\"merchants\":\"Lafayette\",\"transxm\":11,\"CurrentPersonAmt\":10662.631,\"atmTime\":1475437339566,\"atmAmount\":7478.9707,\"atmFrequency\":7,\"atmCurrentPersonAmt\":7478.9707}",
                        "{\"custid\":10013,\"ntranstime\":1475437398688,\"transamt\":10226.88,\"merchants\":\"Harvey Nichols\",\"transxm\":15,\"CurrentPersonAmt\":10226.88,\"atmTime\":1475437331550,\"atmAmount\":7114.0005,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7114.0005}",
                        "{\"custid\":10017,\"ntranstime\":1475437450787,\"transamt\":11638.35,\"merchants\":\"Harvey Nichols\",\"transxm\":16,\"CurrentPersonAmt\":11638.35,\"atmTime\":1475437450787,\"atmAmount\":13887.99,\"atmFrequency\":19,\"atmCurrentPersonAmt\":13887.99}",
                        "{\"custid\":10019,\"ntranstime\":1475437391676,\"transamt\":7576.5703,\"merchants\":\"Harvey Nichols\",\"transxm\":11,\"CurrentPersonAmt\":7576.5703,\"atmTime\":1475437207311,\"atmAmount\":10627.9,\"atmFrequency\":11,\"atmCurrentPersonAmt\":10627.9}",
                        "{\"custid\":10015,\"ntranstime\":1475437422734,\"transamt\":14699.88,\"merchants\":\"Harvey Nichols\",\"transxm\":17,\"CurrentPersonAmt\":14699.88,\"atmTime\":1475437422734,\"atmAmount\":11425.541,\"atmFrequency\":11,\"atmCurrentPersonAmt\":11425.541}",
                        "{\"custid\":10008,\"ntranstime\":1475437412716,\"transamt\":4864.04,\"merchants\":\"Harvey Nichols\",\"transxm\":7,\"CurrentPersonAmt\":4864.04,\"atmTime\":1475437289468,\"atmAmount\":7952.57,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7952.57}",
                        "{\"custid\":10020,\"ntranstime\":1475437433754,\"transamt\":8433.239,\"merchants\":\"Lafayette\",\"transxm\":12,\"CurrentPersonAmt\":8433.239,\"atmTime\":1475437428744,\"atmAmount\":10674.0205,\"atmFrequency\":13,\"atmCurrentPersonAmt\":10674.0205}",
                        "{\"custid\":10006,\"ntranstime\":1475437437762,\"transamt\":12451.201,\"merchants\":\"Carrefour\",\"transxm\":18,\"CurrentPersonAmt\":12451.201,\"atmTime\":1475437342572,\"atmAmount\":5672.7197,\"atmFrequency\":9,\"atmCurrentPersonAmt\":5672.7197}",
                        "{\"custid\":10004,\"ntranstime\":1475437444776,\"transamt\":11164.301,\"merchants\":\"La Rlnascente\",\"transxm\":15,\"CurrentPersonAmt\":11164.301,\"atmTime\":1475437358612,\"atmAmount\":5574.6196,\"atmFrequency\":10,\"atmCurrentPersonAmt\":5574.6196}",
                        "{\"custid\":10011,\"ntranstime\":1475437400693,\"transamt\":14450.76,\"merchants\":\"Lafayette\",\"transxm\":17,\"CurrentPersonAmt\":14450.76,\"atmTime\":1475437389672,\"atmAmount\":9020.359,\"atmFrequency\":10,\"atmCurrentPersonAmt\":9020.359}",
                        "{\"custid\":10002,\"ntranstime\":1475437445778,\"transamt\":10803.89,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10803.89,\"atmTime\":1475437451789,\"atmAmount\":8754.38,\"atmFrequency\":11,\"atmCurrentPersonAmt\":8754.38}"
                };
        return jsonArr;
    }

    private String[] getJsonArr2() {
        String[] jsonArr =
                {
                        "{\"_id\":\"wkz10007\",\"custid\":10007,\"ntranstime\":1475437449785,\"transamt\":10222.14,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10222.14,\"atmTime\":1475437449785,\"atmAmount\":3923.15,\"atmFrequency\":6,\"atmCurrentPersonAmt\":3923.15}",
                        "{\"_id\":\"wkz10010\",\"custid\":10010,\"ntranstime\":1475437446779,\"transamt\":8305.7705,\"merchants\":\"Carrefour\",\"transxm\":10,\"CurrentPersonAmt\":8305.7705,\"atmTime\":1475437416723,\"atmAmount\":5747.5996,\"atmFrequency\":6,\"atmCurrentPersonAmt\":5747.5996}",
                        "{\"_id\":\"wkz10001\",\"custid\":10001,\"ntranstime\":1475437442772,\"transamt\":3490.9697,\"merchants\":\"La Rlnascente\",\"transxm\":5,\"CurrentPersonAmt\":3490.9697,\"atmTime\":1475437438764,\"atmAmount\":6408.8696,\"atmFrequency\":10,\"atmCurrentPersonAmt\":6408.8696}",
                        "{\"_id\":\"wkz10003\",\"custid\":10003,\"ntranstime\":1475437361618,\"transamt\":8757.19,\"merchants\":\"Carrefour\",\"transxm\":14,\"CurrentPersonAmt\":8757.19,\"atmTime\":1475437411714,\"atmAmount\":11501.74,\"atmFrequency\":15,\"atmCurrentPersonAmt\":11501.74}",
                        "{\"_id\":\"wkz10018\",\"custid\":10018,\"ntranstime\":1475437435758,\"transamt\":10456.45,\"merchants\":\"Lafayette\",\"transxm\":15,\"CurrentPersonAmt\":10456.45,\"atmTime\":1475437360616,\"atmAmount\":6614.98,\"atmFrequency\":13,\"atmCurrentPersonAmt\":6614.98}",
                        "{\"_id\":\"wkz10016\",\"custid\":10016,\"ntranstime\":1475437424737,\"transamt\":13565.02,\"merchants\":\"Macy's\",\"transxm\":17,\"CurrentPersonAmt\":13565.02,\"atmTime\":1475437447781,\"atmAmount\":9488.27,\"atmFrequency\":13,\"atmCurrentPersonAmt\":9488.27}",
                        "{\"_id\":\"wkz10009\",\"custid\":10009,\"ntranstime\":1475437420730,\"transamt\":4988.57,\"merchants\":\"Macy's\",\"transxm\":9,\"CurrentPersonAmt\":4988.57,\"atmTime\":1475437431750,\"atmAmount\":7295.47,\"atmFrequency\":9,\"atmCurrentPersonAmt\":7295.47}",
                        "{\"_id\":\"wkz10005\",\"custid\":10005,\"ntranstime\":1475437396684,\"transamt\":13259.92,\"merchants\":\"Harrods\",\"transxm\":17,\"CurrentPersonAmt\":13259.92,\"atmTime\":1475437452791,\"atmAmount\":12974.68,\"atmFrequency\":16,\"atmCurrentPersonAmt\":12974.68}",
                        "{\"_id\":\"wkz10012\",\"custid\":10012,\"ntranstime\":1475437440768,\"transamt\":8120.5903,\"merchants\":\"Macy's\",\"transxm\":10,\"CurrentPersonAmt\":8120.5903,\"atmTime\":1475437374643,\"atmAmount\":11250.45,\"atmFrequency\":13,\"atmCurrentPersonAmt\":11250.45}",
                        "{\"_id\":\"wkz10014\",\"custid\":10014,\"ntranstime\":1475437410712,\"transamt\":10662.631,\"merchants\":\"Lafayette\",\"transxm\":11,\"CurrentPersonAmt\":10662.631,\"atmTime\":1475437339566,\"atmAmount\":7478.9707,\"atmFrequency\":7,\"atmCurrentPersonAmt\":7478.9707}",
                        "{\"_id\":\"wkz10013\",\"custid\":10013,\"ntranstime\":1475437398688,\"transamt\":10226.88,\"merchants\":\"Harvey Nichols\",\"transxm\":15,\"CurrentPersonAmt\":10226.88,\"atmTime\":1475437331550,\"atmAmount\":7114.0005,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7114.0005}",
                        "{\"_id\":\"wkz10017\",\"custid\":10017,\"ntranstime\":1475437450787,\"transamt\":11638.35,\"merchants\":\"Harvey Nichols\",\"transxm\":16,\"CurrentPersonAmt\":11638.35,\"atmTime\":1475437450787,\"atmAmount\":13887.99,\"atmFrequency\":19,\"atmCurrentPersonAmt\":13887.99}",
                        "{\"_id\":\"wkz10019\",\"custid\":10019,\"ntranstime\":1475437391676,\"transamt\":7576.5703,\"merchants\":\"Harvey Nichols\",\"transxm\":11,\"CurrentPersonAmt\":7576.5703,\"atmTime\":1475437207311,\"atmAmount\":10627.9,\"atmFrequency\":11,\"atmCurrentPersonAmt\":10627.9}",
                        "{\"_id\":\"wkz10015\",\"custid\":10015,\"ntranstime\":1475437422734,\"transamt\":14699.88,\"merchants\":\"Harvey Nichols\",\"transxm\":17,\"CurrentPersonAmt\":14699.88,\"atmTime\":1475437422734,\"atmAmount\":11425.541,\"atmFrequency\":11,\"atmCurrentPersonAmt\":11425.541}",
                        "{\"_id\":\"wkz10008\",\"custid\":10008,\"ntranstime\":1475437412716,\"transamt\":4864.04,\"merchants\":\"Harvey Nichols\",\"transxm\":7,\"CurrentPersonAmt\":4864.04,\"atmTime\":1475437289468,\"atmAmount\":7952.57,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7952.57}",
                        "{\"_id\":\"wkz10020\",\"custid\":10020,\"ntranstime\":1475437433754,\"transamt\":8433.239,\"merchants\":\"Lafayette\",\"transxm\":12,\"CurrentPersonAmt\":8433.239,\"atmTime\":1475437428744,\"atmAmount\":10674.0205,\"atmFrequency\":13,\"atmCurrentPersonAmt\":10674.0205}",
                        "{\"_id\":\"wkz10006\",\"custid\":10006,\"ntranstime\":1475437437762,\"transamt\":12451.201,\"merchants\":\"Carrefour\",\"transxm\":18,\"CurrentPersonAmt\":12451.201,\"atmTime\":1475437342572,\"atmAmount\":5672.7197,\"atmFrequency\":9,\"atmCurrentPersonAmt\":5672.7197}",
                        "{\"_id\":\"wkz10004\",\"custid\":10004,\"ntranstime\":1475437444776,\"transamt\":11164.301,\"merchants\":\"La Rlnascente\",\"transxm\":15,\"CurrentPersonAmt\":11164.301,\"atmTime\":1475437358612,\"atmAmount\":5574.6196,\"atmFrequency\":10,\"atmCurrentPersonAmt\":5574.6196}",
                        "{\"_id\":\"wkz10011\",\"custid\":10011,\"ntranstime\":1475437400693,\"transamt\":14450.76,\"merchants\":\"Lafayette\",\"transxm\":17,\"CurrentPersonAmt\":14450.76,\"atmTime\":1475437389672,\"atmAmount\":9020.359,\"atmFrequency\":10,\"atmCurrentPersonAmt\":9020.359}",
                        "{\"_id\":\"wkz10002\",\"custid\":10002,\"ntranstime\":1475437445778,\"transamt\":10803.89,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10803.89,\"atmTime\":1475437451789,\"atmAmount\":8754.38,\"atmFrequency\":11,\"atmCurrentPersonAmt\":8754.38}"
                };
        return jsonArr;
    }

    /**
     * 查询案例2
     */
    @Test
    public void testSearchHuiYi() {
        /**
         数据源：先插入数据源
         curl -XPUT 'http://192.168.2.124:9200/iteblog_book_index' -d '{ "settings": { "number_of_shards": 1 }}'

         curl -XPOST 'http://192.168.2.124:9200/iteblog_book_index/book/_bulk' -d '
         { "index": { "_id": 1 }}
         { "title": "Elasticsearch: The Definitive Guide", "authors": ["clinton gormley", "zachary tong"], "summary" : "A distibuted real-time search and analytics engine", "publish_date" : "2015-02-07", "num_reviews": 20, "publisher": "oreilly" }
         { "index": { "_id": 2 }}
         { "title": "Taming Text: How to Find, Organize, and Manipulate It", "authors": ["grant ingersoll", "thomas morton", "drew farris"], "summary" : "organize text using approaches such as full-text search, proper name recognition, clustering, tagging, information extraction, and summarization", "publish_date" : "2013-01-24", "num_reviews": 12, "publisher": "manning" }
         { "index": { "_id": 3 }}
         { "title": "Elasticsearch in Action", "authors": ["radu gheorge", "matthew lee hinman", "roy russo"], "summary" : "build scalable search applications using Elasticsearch without having to do complex low-level programming or understand advanced data science algorithms", "publish_date" : "2015-12-03", "num_reviews": 18, "publisher": "manning" }
         { "index": { "_id": 4 }}
         { "title": "Solr in Action", "authors": ["trey grainger", "timothy potter"], "summary" : "Comprehensive guide to implementing a scalable search engine using Apache Solr", "publish_date" : "2014-04-05", "num_reviews": 23, "publisher": "manning" }'


         * */


        String index = "iteblog_book_index";
        String type = "book";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        SortBuilder[] sorts = null;


        //所有字段中搜索guid关键字，不区分大小写。
        query = QueryBuilders.multiMatchQuery("guide", "_all");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。



        //title字段中搜索“in action”,默认会对它分词进行搜索
        //query = QueryBuilders.matchPhraseQuery("title", "in action");//精确匹配
        query = QueryBuilders.matchQuery("title", "in action");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //title,summer字段中搜索,并讲summer字段权重设置为3
        query = QueryBuilders.multiMatchQuery("elasticsearch guide", "title", "summary^3");
        query = QueryBuilders.multiMatchQuery("elasticsearch guide").field("title").field("summary", 3.0f);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //布尔查询（与或非）
        query = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery("title", "elasticsearch"))
                        .should(QueryBuilders.matchQuery("title", "Solr")))
                .must(QueryBuilders.matchQuery("authors", "clinton gormely"))
                .mustNot(QueryBuilders.matchQuery("authors", "radu gheorge"));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //分词后的模糊查询【fuzziness("AUTO")，自动设置编辑距离，不区分大小写】
        query = QueryBuilders.multiMatchQuery("comprihensiv guide", "title", "summmary").fuzziness("AUTO");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //通配符查询(区分大小写)
        ///**query = QueryBuilders.wildcardQuery("code","*M003808D01*"))//类似于sql中的like '%M003808D01%'//**/
        query = QueryBuilders.wildcardQuery("authors", "T*");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //正则表达式查询(区分大小写)
        query = QueryBuilders.regexpQuery("authors", "t[a-z]*y");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //匹配短语查询(查询不到数据？？？)
        query = QueryBuilders.multiMatchQuery("search engine", "title", "summmary").type(MultiMatchQueryBuilder.Type.PHRASE).slop(3);

        //匹配短语前缀查询(匹配短语前缀查询是有性能消耗的，所有使用之前需要小心。)
        query = QueryBuilders.matchPhrasePrefixQuery("summary", "search en").slop(3).maxExpansions(10);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //根据分词后进行模糊查询
        //例子中，我们运行了一个模糊搜索(fuzzy search)，搜索关键字是"saerch algorithm"，
        //并且作者包含grant ingersoll或者tom morton。并且搜索了所有的字段，其中summary字段的权重为2：
        query = QueryBuilders.queryStringQuery("(saerch~1 algorithm~1) AND (grant ingersoll) OR (tom morton)").field("_all").field("summary", 2.0f);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //这个是queryString的另外版本，使用+/|/- 分别替换AND/OR/NOT，如果用输入了错误的查询，其直接忽略这种情况而不是抛出异常
        query = QueryBuilders.simpleQueryStringQuery("(saerch~1 algorithm~1) + (grant ingersoll)  | (tom morton)").field("_all").field("summary", 2.0f);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.termQuery("publisher", "manning");
        sorts = new SortBuilder[]{
                SortBuilders.fieldSort("publish_date").order(SortOrder.DESC),
                SortBuilders.fieldSort("title").order(SortOrder.DESC)
        };
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.termsQuery("publisher", "oreilly", "packt");      //2个字段的是值是或的关系
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //范围查询：应用于日期，数字以及字符类型的字段。【左右都闭合】
        query = QueryBuilders.rangeQuery("publish_date").gte("2015-01-01").lte("2015-12-31");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //过滤查询：在实际运用中，过滤器应该先被执行，这样可以减少需要查询的范围。而且，第一次使用fliter之后其将会被缓存，这样会对性能代理提升。
        //第1种方式：已弃用
        query = QueryBuilders.filteredQuery(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"),
                QueryBuilders.rangeQuery("num_reviews").gte(20));
        //第2种方式：
        query = QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary");
        filter = QueryBuilders.rangeQuery("num_reviews").gte(20);
        //第3种方式：
        query = QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"))
                .filter(QueryBuilders.rangeQuery("num_reviews").gte(20));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.filteredQuery(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"),
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("num_reviews").gte("20"))
                        .mustNot(QueryBuilders.rangeQuery("publish_date").lte("2014-12-31"))
                        .must(QueryBuilders.termQuery("publisher", "oreilly")));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //在某些场景下，你可能想对某个特定字段设置一个因子(factor)，并通过这个因子计算某个文档的相关度(relevance score)。
        //这是典型地基于文档(document)的重要性来抬高其相关性的方式。
        //在下面例子中，我们想找到更受欢迎的图书(是通过图书的评论实现的)，并将其权重抬高，这里通过使用FieldValueFactor来实现。
        query = QueryBuilders.functionScoreQuery(QueryBuilders.multiMatchQuery("search engine", "title", "summary"))
                .add(ScoreFunctionBuilders.fieldValueFactorFunction("num_reviews").factor(2.0f).modifier(FieldValueFactorFunction.Modifier.LOG1P));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。


        //参考：https://www.iteblog.com/archives/1768
        //gauss 衰减速度先慢后快再慢，exp 衰减速度先快后慢，lin 直线衰减，在0分外的值都是0分，如何选择取决于你想要你的score以什么速度衰减。
        //下面例子中我们搜索标题或者摘要中包含search engines的图书，并且希望图书的发行日期是在2014-06-15中心点范围内，如下：
        query = QueryBuilders.functionScoreQuery(QueryBuilders.multiMatchQuery("search engine", "title", "summary"))
                .add(ScoreFunctionBuilders.exponentialDecayFunction("publish_date", "2014-06-15", "30d").setOffset("7d"))//fieldName,origin,scale,offset
                .boostMode(CombineFunction.REPLACE);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-24 上午11:32
     * 名称：分组查询
     * 备注：
     */
    @Test
    public void testSearchAgg() {
        index = "newkangkang";
        type = "newdata";

        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        String[] aggNames = null;
        AbstractAggregationBuilder[] aggBuilders = null;
        SearchResponse response = null;

//        /**
//         * 根据时间进行分组
//         * */
//        aggNames = new String[]{"birthday"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                //                AggregationBuilders.terms(aggNames[0]).field("merchants"),
//                AggregationBuilders.dateHistogram(aggNames[0]).field("birthday").interval(DateHistogramInterval.YEAR)
//                //                AggregationBuilders.avg(aggNames[1]).field("custid")
//                //                AggregationBuilders.terms("by_country").field("country").subAggregation(
//                //                        AggregationBuilders.dateHistogram("by_year").field("dateOfBirth").interval((DateHistogramInterval.YEAR)).subAggregation(
//                //                                AggregationBuilders.avg("avg_children").field("children")
//                //                        )
//                //                )
//        };
//        response = MyWork.ESUtils.operSearchAggWithNoDel(new String[]{"testcreateindex"}, new String[]{"testtype"}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            System.out.println(i);
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            InternalHistogram agg = (InternalHistogram) aggregation;
//            List<InternalHistogram.Bucket> buckets = agg.getBuckets();
//            for (InternalHistogram.Bucket bucket : buckets) {
//                System.out.println("key:" + bucket.getKeyAsString() + "\tcount:" + bucket.getDocCount());
//                Aggregations aggregations = bucket.getAggregations();
//                ValueFormatter formatter = bucket.getFormatter();
//                boolean keyed = bucket.getKeyed();
//            }
//        }
        aggNames = new String[]{"atmFrequency"};
        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.terms(aggNames[0]).field("atmFrequency")
                AggregationBuilders.cardinality(aggNames[0]).field(aggNames[0])
        };
        response = ESUtils.operSearchAggWithNoDel(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
            InternalCardinality car = (InternalCardinality) aggregation;
            System.out.println(car);

//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                     // bucket key
//                long docCount = entry.getDocCount();                        // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//            }
        }
//        key:10, doc_count:5
//        key:13, doc_count:4
//        key:11, doc_count:3
//        key:6, doc_count:2
//        key:9, doc_count:2
//        key:7, doc_count:1
//        key:15, doc_count:1
//        key:16, doc_count:1
//        key:19, doc_count:1

//
//
//        /**
//         * 常用聚合函数
//         * */
//        aggNames = new String[]{"transxm_Min", "atmFrequency_Max",};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.min(aggNames[0]).field("transxm"),
//                AggregationBuilders.max(aggNames[1]).field("atmFrequency")
//                //AggregationBuilders.sum(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.avg(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.count(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.cardinality(aggNames[0]).field("atmFrequency")      //基数
//        };
//        response = MyWork.ESUtils.operSearchAggWithNoDel(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            if (i == 0) {
//                Min agg = (Min) aggregation;
//                System.out.println(agg.getName() + ":" + agg.getValue());
//            } else {
//                Max agg = (Max) aggregation;
//                System.out.println(agg.getName() + ":" + agg.getValue());
//            }
//            //Sum agg = (Sum) aggregation;
//            //Avg agg = (Avg) aggregation;
//            //ValueCount agg = (ValueCount) aggregation;
//            //Cardinality agg = (Cardinality) aggregation;
//        }
//
//
//        /**
//         * 常用聚合函数和常用统计函数
//         * */
//        aggNames = new String[]{"atmFrequency_stats", "transxm_extendedStats"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.stats(aggNames[0]).field("atmFrequency"),
//                AggregationBuilders.extendedStats(aggNames[1]).field("transxm")
//                //AggregationBuilders.percentiles(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.percentiles(aggNames[0]).field("atmFrequency").percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0)//后面参数是指 抽取百分之多少 数据。
//                //AggregationBuilders.percentileRanks(aggNames[0]).field("atmFrequency").percentiles(1, 50, 75)
//                ////获取边界【经纬度】
//                //AggregationBuilders.geoBounds(aggNames[0]).field("merchants")
//        };
//        response = MyWork.ESUtils.operSearchAggWithNoDel(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            if (i == 0) {
//                Stats stats = (Stats) aggregation;
//                System.out.println(stats.getName() +
//                        ":\tmin:" + stats.getMinAsString() +
//                        "\tmax:" + stats.getMaxAsString() +
//                        "\tsum:" + stats.getSumAsString() +
//                        "\tavg:" + stats.getAvgAsString() +
//                        "\tcount:" + stats.getCountAsString());
//            } else {
//                ExtendedStats extendedStats = response.getAggregations().get(aggNames[i]);
//                System.out.println(extendedStats.getName() +
//                        ":\tmin:" + extendedStats.getMinAsString() +
//                        "\tmax:" + extendedStats.getMaxAsString() +
//                        "\tsum:" + extendedStats.getSumAsString() +
//                        "\tavg:" + extendedStats.getAvgAsString() +
//                        "\tcount:" + extendedStats.getCountAsString() +
//                        "\tstdDeviation:" + extendedStats.getStdDeviationAsString() +  //标准差
//                        "\tsumOfSquares:" + extendedStats.getSumOfSquaresAsString() +  //平方和
//                        "\tvariance:" + extendedStats.getVarianceAsString());         //方差
//            }
//        }
//
//        /**
//         * 分组之后聚合操作(.size(3)是分组之后显示的条数)
//         * */
//        aggNames = new String[]{"atmFrequency_top", "atmFrequency_sum", "atmFrequency_avg", "atmFrequency_count"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                //分组取几条数据（默认是取3条数据:.setFrom(0).setSize(3)）
//                //AggregationBuilders.terms(aggNames[0]).field("atmFrequency").subAggregation(AggregationBuilders.topHits("top"))
//                AggregationBuilders.terms(aggNames[0]).field("atmFrequency").size(3).subAggregation(
//                        AggregationBuilders.topHits("top").setExplain(false).setFrom(0).setSize(3)),
//                //分组在求和，根据分组字段进行升序
//                AggregationBuilders.terms(aggNames[1]).field("atmFrequency").subAggregation(
//                        AggregationBuilders.sum("sum").field("transxm")
//                ).order(Terms.Order.term(true)),
//                //分组在求平均/最小值/最大值，根据平均值/最小值/最大值进行降序
//                AggregationBuilders.terms(aggNames[2]).field("atmFrequency").subAggregation(
//                        AggregationBuilders.avg("avg").field("transxm")
//                        //AggregationBuilders.min("min").field("transxm")
//                        //AggregationBuilders.max("max").field("transxm")
//                ).order(Terms.Order.aggregation("avg", true)),
//                //分组在求和，根据分组字段里'文档个数总数'进行升序
//                AggregationBuilders.terms(aggNames[3]).field("atmFrequency").order(Terms.Order.count(true))
//        };
//        response = MyWork.ESUtils.operSearchAggWithNoDel(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            System.out.println(i);
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                     // bucket key
//                long docCount = entry.getDocCount();                        // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                //求top
//                if (i == 0) {
//                    TopHits topHits = entry.getAggregations().get("top");
//                    for (SearchHit hit : topHits.getHits().getHits()) {
//                        System.out.println(" -> id:" + hit.getId() + ", _source" + hit.getSourceAsString());
//                    }
//                } else if (i == 1) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    Sum chiHit = entry.getAggregations().get("sum");
//                    System.out.println("sum:" + chiHit.getValue());
//                } else if (i == 2) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    Avg chiHit = entry.getAggregations().get("avg");
//                    //Min chiHit = entry.getAggregations().get("min");
//                    //Max chiHit = entry.getAggregations().get("max");
//                    System.out.println("avg:" + chiHit.getValue());
//                } else if (i == 3) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    System.out.println("count:" + entry.getDocCount());
//                }
//            }
//        }


        //分组再分组再统计
        //aggBuilders = new AbstractAggregationBuilder[]{
        //        AggregationBuilders.terms(aggNames[0]).field("merchants"),
        //        AggregationBuilders.terms("by_country").field("country").subAggregation(
        //                AggregationBuilders.dateHistogram("by_year").field("dateOfBirth").interval((DateHistogramInterval.YEAR)).subAggregation(
        //                        AggregationBuilders.avg("avg_children").field("children")
        //                )
        //        )
        //};
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-21 下午7:03
     * 名称：父子文档查询
     * 备注：
     * 插入数据的时候要先建立索引
     * 参考：http://blog.csdn.net/napoay/article/details/52032931
     */
    @Test
    public void testSearchParent() {
        /**
         * 数据源:

         curl -XPUT  "http://192.168.2.124:9200/parent" -d '
         {
         "mappings": {
         "branch": {},
         "employee": {
         "_parent": {
         "type": "branch"
         }
         }
         }
         }'

         //父文档
         curl -XPOST 'http://192.168.2.124:9200/parent/branch/_bulk' -d '
         { "index": { "_id": "london" }}
         { "name": "London Westminster", "city": "London", "country": "UK" }
         { "index": { "_id": "liverpool" }}
         { "name": "Liverpool Central", "city": "Liverpool", "country": "UK" }
         { "index": { "_id": "paris" }}
         { "name": "Champs Élysées", "city": "Paris", "country": "France" }
         '

         //子文档
         curl -XPUT "http://192.168.2.124:9200/parent/employee/1?parent=london&pretty" -d '
         {
         "name":  "Alice Smith",
         "dob":   "1970-10-24",
         "hobby": "hiking"
         }'

         curl -XPOST 'http://192.168.2.124:9200/parent/employee/_bulk' -d '
         { "index": { "_id": 2, "parent": "london" }}
         { "name": "Mark Thomas", "dob": "1982-05-16", "hobby": "diving" }
         { "index": { "_id": 3, "parent": "liverpool" }}
         { "name": "Barry Smith", "dob": "1979-04-01", "hobby": "hiking" }
         { "index": { "_id": 4, "parent": "paris" }}
         { "name": "Adrien Grand", "dob": "1987-05-11", "hobby": "horses" }
         '

         * **/


        String index = "parent";
        String parType = "branch";
        String chiType = "employee";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        //SortBuilder sort = SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
        SortBuilder[] sorts = null;


        //通过子文档查询条件 显示相关父文档
        //搜索含有1980年以后出生的employee的branch
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.rangeQuery("dob").gte("1980-01-01"));//【子文档type,子文档查询条件】
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //查询name中含有“Alice Smith”的branch
        //定义里嵌套对象计算的分数与当前查询分数的处理方式，有avg,sum,max,min以及none。none就是不做任何处理，其他的看字面意思就好理解。
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.matchQuery("name", "Alice Smith")).scoreMode("avg");
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //搜索最少含有2个employee的branch：
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.matchAllQuery()).minChildren(2);
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.

        //通过父文档查询条件 显示相关子文档
        query = QueryBuilders.hasParentQuery("branch", QueryBuilders.matchQuery("country", "UK"));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{chiType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //搜索有相同父id的子文档：搜索父id为"liverpool","paris"的employee：
        query = QueryBuilders.hasParentQuery("branch", QueryBuilders.idsQuery().ids("liverpool", "paris"));
        ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{chiType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.


        ////三级父子关系的嵌套搜索【事例，并无数据】
        //TermsLookupQueryBuilder three = QueryBuilders.termsLookupQuery("uuid").lookupIndex("user").lookupType("user").lookupId("5").lookupPath("uuids");
        //HasChildQueryBuilder two = QueryBuilders.hasChildQuery("graType",three);//用孙子type
        //HasChildQueryBuilder one = QueryBuilders.hasChildQuery("chiType", two);//用儿子type
        //OldESUtils.operSearchWithNoDel(new String[]{index}, new String[]{"parType"}, query, filter,fields, sorts, 0, 0);//用父type


        //********************************************************************
        //对关联的 child 文档进行聚合操作[父type分组之后在子type再分组]（参考：http://www.cnblogs.com/licongyu/p/5557693.html）
        //先分组父type的国家，在分组子type的嗜好
        //********************************************************************
        String[] aggNames = {"country"};
        AbstractAggregationBuilder[] aggBuilders = {
                AggregationBuilders.terms(aggNames[0]).field("country").subAggregation(                 //这里设置根据父表的哪个字段进行分组
                        AggregationBuilders.children("employee").childType("employee").subAggregation(      //这里设置父表的子表
                                AggregationBuilders.terms("hobby").field("hobby")                           //这里设置根据子表的哪个字段进行分组
                        )
                )

        };
        SearchResponse response = ESUtils.operSearchAggWithNoDel(new String[]{index}, new String[]{parType}, query, filter, aggBuilders);//这里设置父表type
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
            //********************************************************************
            //父子表中：先根据父表的country分组，在设置子表，在根据子表的hobby进行分组。
            //********************************************************************
            Terms terms = (Terms) aggregation;
            for (Terms.Bucket entry : terms.getBuckets()) {
                String key = entry.getKeyAsString();
                long docCount = entry.getDocCount();
                System.out.println("key:" + key + ", doc_count:" + docCount);
                Children children = entry.getAggregations().get("employee");
                Terms chiTerms = children.getAggregations().get("hobby");
                for (Terms.Bucket chiEntry : chiTerms.getBuckets()) {
                    key = chiEntry.getKeyAsString();
                    docCount = chiEntry.getDocCount();
                    System.out.println("\t--> key:" + key + ", doc_count:" + docCount);
                }
            }
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/6 11:35
     * 名称：关联查询[跟父子文档查询类似,但不一样,它这个不是父子关系]
     * 备注：
     */
    @Test
    public void testSearchSiren() {
        /**
         * 数据源:
         curl -XPUT 'http://192.168.2.124:9200/_bulk?pretty' -d '
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "1" } }
         { "title" : "The NoSQL database glut", "mentions" : ["1", "2"] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "2" } }
         { "title" : "Graph Databases Seen Connecting the Dots", "mentions" : [] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "3" } }
         { "title" : "How to determine which NoSQL DBMS best fits your needs", "mentions" : ["2", "4"] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "4" } }
         { "title" : "MapR ships Apache Drill", "mentions" : ["4"] }

         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "1" } }
         { "id": "1", "name" : "Elastic" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "2" } }
         { "id": "2", "name" : "Orient Technologies" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "3" } }
         { "id": "3", "name" : "Cloudera" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "4" } }
         { "id": "4", "name" : "MapR" }
         '

         * **/

        index = "companies";
        type = "company";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = QueryBuilders.matchAllQuery();
        SortBuilder[] sorts = null;
        filter = QueryBuilders.boolQuery().filter(
                //id属于company,mentions属于article;id和mentions是相关联的
                filterJoin("id").indices("articles").types("article").path("mentions").query(
                        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("title", "nosql"))
                )
                        .orderBy("default")//只有2种排序方式:default,doc_score
                        .maxTermsPerShard(5)//每个分片存储最多分组数量
        );
        //下面这个是简洁写法
        //filter=filterJoin("id").indices("articles").types("article").path("mentions").query(
        //        QueryBuilders.termQuery("title", "nosql")
        //);
        //查询出来只有type的所有字段
        ESUtils.operSearchSiren(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);


//        index = "parent";
//        String parType = "branch";
//        String chiType = "employee";
//        ////通过子文档查询条件 显示相关父文档
//        ////搜索含有1980年以后出生的employee的branch
//        //query = QueryBuilders.hasChildQuery("employee", QueryBuilders.rangeQuery("dob").gte("1980-01-01"));//【子文档type,子文档查询条件】
//        //MyWork.ESUtils.operSearchWithNoDel(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
//        //将上面转化为siren形式查询,转化失败.父子表查询是根据父表的_id来关联的.siren方式则不允许!!!
//        filter=QueryBuilders.boolQuery().filter(filterJoin("_id").indices(index).types(chiType).path("parent").query(
//                QueryBuilders.rangeQuery("dob").gte("1980-01-01")
//        ));
//        MyWork.ESUtils.operSearchSiren(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);
    }

    class Person {
        public Person(String name, int age, String birthday, List<Book> books) {
            this.name = name;
            this.age = age;
            this.books = books;
            this.birthday = birthday;
        }

        public Person(String name, int age, List<Book> books) {
            this.name = name;
            this.age = age;
            this.books = books;
        }

        private int age;
        private String name;
        private List<Book> books;
        private String birthday;

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Book> getBooks() {
            return books;
        }

        public void setBooks(List<Book> books) {
            this.books = books;
        }

        public String getBirthday() {
            return birthday;
        }

        public void setBirthday(String birthday) {
            this.birthday = birthday;
        }
    }

    class Book {
        public Book(String name, int price) {
            this.name = name;
            this.price = price;
        }

        private int price;
        private String name;

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}