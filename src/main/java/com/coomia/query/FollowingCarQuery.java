/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.coomia.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 车辆跟随查询
 *
 * @author spancer.ray
 */
public class FollowingCarQuery {

  public static void main(String[] args) throws IOException {

    long start = 1609430400000L; // 2021-01-01
    long end = 1610640000000L; // 2021-01-15
    Integer[] areaA = new Integer[] {0, 1, 3, 4, 5, 6}; // deviceIDs of areaA
    Integer[] areaB = new Integer[] {7, 8, 9, 10, 11}; // deviceIDs of areaB
    String[] deviceIds = {"36", "998"};
    String plateNo = null;
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost("elasticsearch", 9200, "http")));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    BoolQueryBuilder query =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end))
            .must(QueryBuilders.termsQuery("DeviceID", deviceIds));

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("areaA", new ArrayList<Integer>(Arrays.asList(areaA)));
    params.put("areaB", new ArrayList<Integer>(Arrays.asList(areaB)));
    Script init =
        new Script(
            ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, " state.a1 =[]; state.a2 =[];", params);
    Script map =
        new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            "if(params.areaA.contains(doc.DeviceID.value.intValue()))state.a1.add(doc.PlateNo.value); else if(params.areaB.contains(doc.DeviceID.value.intValue())) state.a2.add(doc.PlateNo.value);",
            params);
    Script combine =
        new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            "List profit = []; List profit2 = [];  for (t in state.a1) { profit.add(t)}  for (t in state.a2) { profit2.add(t)} profit.retainAll(profit2) ; return new HashSet(profit).toArray();",
            params);
    Script reduce =
        new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            "List a=[];  for(s in states ) { for(t in s){ a.add(t)} }  return new HashSet(a);",
            params);
    ScriptedMetricAggregationBuilder smab =
        AggregationBuilders.scriptedMetric("CollisionCar")
            .initScript(init)
            .mapScript(map)
            .combineScript(combine)
            .reduceScript(reduce);

    ssb.aggregation(
        AggregationBuilders.terms("GroupbyDeviceID")
            .field("DeviceID")
            .subAggregation(
                AggregationBuilders.terms("GroupbyPlateNo")
                    .field("PlateNo")
                    .subAggregation(smab)));
    ssb.query(query);
    ssb.size(0);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("car");
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    long took = sr.getTook().getMillis();
    System.out.println("耗时：" + took);

    Aggregations aggRes = sr.getAggregations();
    Terms devices = aggRes.get("GroupbyPlateNo");
    List<? extends Terms.Bucket> data = devices.getBuckets();
    for (Terms.Bucket bks : data) {
      // 输出是为了看看各个值是多少，查询本身输出的数据，就是套牌车了。
      System.out.println("套牌车: " + bks.getKey());
      System.out.println(
          "PlateNo: "
              + bks.getKey()
              + " plateColorDescDistinct: "
              + ((ParsedCardinality) bks.getAggregations().get("plateColorDescDistinct")).getValue()
              + " vehicleClassDescDistinct: "
              + ((ParsedCardinality) bks.getAggregations().get("vehicleClassDescDistinct"))
                  .getValue()
              + " VehicleBrandDistinct:"
              + ((ParsedCardinality) bks.getAggregations().get("VehicleBrandDistinct")).getValue()
              + " SpeedMetrics  "
              + ((ParsedScriptedMetric) bks.getAggregations().get("SpeedMetrics")).aggregation());
    }

    client.close();
  }
}
