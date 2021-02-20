
/**
 * Project Name:flink-es-sink-demo File Name:ESQueryDemo.java Package Name:com.coomia.query
 * Date:2020年9月9日下午5:25:37 Copyright (c) 2020, spancer.ray All Rights Reserved.
 *
 */

package com.coomia.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 车辆碰撞分析
 * 
 * select plateNo from t where shotTime between t1 and t2 and DeviceID in (d1,d2,d3) group by
 * DeviceID, PlateNo having count(PlateNo) > 1
 * 
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class CollisionCarDetectQuery {

  public static void main(String[] args) throws IOException {

    String host = "127.0.0.1";
    int port = 9200;
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    /**
     * query devices in areaA & areaB, along with time range in start & end.
     */
    long start = 1580077247735L; // time start in mills
    long end = 1599255299742L; // time end in mills
    Integer[] areaA = new Integer[] {0, 1, 3, 4, 5, 6}; // deviceIDs of areaA
    Integer[] areaB = new Integer[] {7, 8, 9, 10, 11}; // deviceIDs of areaB
    QueryBuilder orQuery =
        QueryBuilders.boolQuery().should(QueryBuilders.termsQuery("DeviceID", areaA)) 
            .should(QueryBuilders.termsQuery("DeviceID", areaB));
    QueryBuilder query = QueryBuilders.boolQuery()
        .must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end)).must(orQuery);
    ssb.query(query);
    /**
     * calculate the  two result data (two collections), and get the intersection of the both.
     */
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("areaA", new ArrayList<Integer>(Arrays.asList(areaA)));
    params.put("areaB", new ArrayList<Integer>(Arrays.asList(areaB)));
    Script init = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
        " state.a1 =[]; state.a2 =[];", params);
    Script map = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
        "if(params.areaA.contains(doc.DeviceID.value.intValue()))state.a1.add(doc.PlateNo.value); else if(params.areaB.contains(doc.DeviceID.value.intValue())) state.a2.add(doc.PlateNo.value);",
        params);
    Script combine = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
        "List profit = []; List profit2 = [];  for (t in state.a1) { profit.add(t)}  for (t in state.a2) { profit2.add(t)} profit.retainAll(profit2) ; return new HashSet(profit).toArray();",
        params);
    Script reduce = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
        "List a=[];  for(s in states ) { for(t in s){ a.add(t)} }  return new HashSet(a);", params);
    ScriptedMetricAggregationBuilder smab = AggregationBuilders.scriptedMetric("CollisionCar")
        .initScript(init).mapScript(map).combineScript(combine).reduceScript(reduce);
    ssb.size(0);
    ssb.aggregation(smab);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("car"); //index name
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHits hits = sr.getHits();
    long took = sr.getTook().getMillis();
    TotalHits totalHits = hits.getTotalHits();
    System.out.println("耗时：" + took + ",count:" + totalHits.value);

    Aggregations aggRes = sr.getAggregations();
    ParsedScriptedMetric devices = aggRes.get("CollisionCar");
    Object result = devices.aggregation();
    if (null != result) {
      List<Object> data = (List) result;
      data.forEach(item -> System.out.println(item));
    }
    client.close();
  }

}

