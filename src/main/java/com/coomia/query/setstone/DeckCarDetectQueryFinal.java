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
package com.coomia.query.setstone;

import java.io.IOException;
import java.util.ArrayList;
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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ClassName:DeckCarDetectQueryFinal Function: 通过套牌车辆分析，可及时掌握套牌情况，加强对公共安全的管控。
 * 不同车身、不同车型、不同品牌、不同区域(deviceID)同时出现(within time interval) Date: 2020年9月15日 上午9:02:49
 * 该实现应该是最优方案，将速度的计算算在ES索引的过程中进行，为每条记录打上一个速度。
 *
 * @author spancer.ray
 * @version
 * @since JDK 1.8
 * @see
 */
public class DeckCarDetectQueryFinal {

  public static void main(String[] args) throws IOException {

    long start = 1609430400000L; // 2021-01-01
    long end = 1610640000000L; // 2021-01-15
    String plateNo = null;
    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(new HttpHost("elasticsearch", 9200, "http")));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    BoolQueryBuilder query =
        QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end));
    if (null != plateNo) query.must(QueryBuilders.termsQuery("PlateNo", plateNo));
    FilterAggregationBuilder color =
        AggregationBuilders.filter(
                "colorNotNull",
                QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("plateColorDesc", "未知")))
            .subAggregation(
                AggregationBuilders.cardinality("plateColorDescDistinct").field("plateColorDesc"));
    FilterAggregationBuilder clas =
        AggregationBuilders.filter(
                "clasNotNull",
                QueryBuilders.boolQuery()
                    .mustNot(QueryBuilders.termsQuery("vehicleClassDesc", "未知")))
            .subAggregation(
                AggregationBuilders.cardinality("vehicleClassDescDistinct")
                    .field("vehicleClassDesc"));
    FilterAggregationBuilder brand =
        AggregationBuilders.filter(
                "brandNotNull",
                QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("VehicleBrand", "未知")))
            .subAggregation(
                AggregationBuilders.cardinality("vehicleBrandDistinct").field("VehicleBrand"));
    List fields = new ArrayList<String>();
    fields.add("shotTime");
    fields.add("location");

    MaxAggregationBuilder maxspeedAgg =
        AggregationBuilders.max("maxspeed").field("speed"); // speed为车速字段

    /** build script and params. */
    Map<String, String> bucketsPathsMap = new HashMap<String, String>();
    bucketsPathsMap.put("plateColorDescDistinct", "colorNotNull.plateColorDescDistinct");
    bucketsPathsMap.put("vehicleClassDescDistinct", "clasNotNull.vehicleClassDescDistinct");
    bucketsPathsMap.put("vehicleBrandDistinct", "brandNotNull.vehicleBrandDistinct");
    bucketsPathsMap.put("maxspeed", "maxspeed");
    Map<String, Object> havingScriptParam = new HashMap<String, Object>();
    havingScriptParam.put("havingCount", 1); // distinct count > 1，即表示有重复的数据（不同颜色或不同型号或其它）
    havingScriptParam.put("speed", 0.5); // 配置要大于的车速为120
    Script script =
        new Script(
            ScriptType.INLINE,
            "expression",
            "plateColorDescDistinct >havingCount || vehicleClassDescDistinct >havingCount || vehicleBrandDistinct > havingCount || maxspeed>speed",
            havingScriptParam);
    BucketSelectorPipelineAggregationBuilder having =
        PipelineAggregatorBuilders.bucketSelector("HavingPlateNoGT1", bucketsPathsMap, script);
    ssb.aggregation(
        AggregationBuilders.terms("GroupbyPlateNo")
            .field("PlateNo")
            .subAggregation(color)
            .subAggregation(clas)
            .subAggregation(brand)
            .subAggregation(maxspeedAgg)
            .subAggregation(having));
    ssb.query(query);
    ssb.size(0);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("cartimeasc");
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    long took = sr.getTook().getMillis();
    System.out.println("耗时：" + took);

    Aggregations aggRes = sr.getAggregations();
    Terms devices = aggRes.get("GroupbyPlateNo");
    List<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> data =
        devices.getBuckets();
    for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bks : data) {
      // 输出是为了看看各个值是多少，查询本身输出的数据，就是套牌车了。
      System.out.println("套牌车: " + bks.getKey());
      for (Aggregation sub : bks.getAggregations()) {
        String name = sub.getName();
        if (name.equals("colorNotNull"))
          System.out.print(
              " color: "
                  + ((ParsedCardinality)
                          ((ParsedFilter) sub).getAggregations().get("plateColorDescDistinct"))
                      .getValue());
        else if (name.equals("clasNotNull"))
          System.out.print(
              " clas: "
                  + ((ParsedCardinality)
                          ((ParsedFilter) sub).getAggregations().get("vehicleClassDescDistinct"))
                      .getValue());
        else if (name.equals("brandNotNull"))
          System.out.print(
              " brand: "
                  + ((ParsedCardinality)
                          ((ParsedFilter) sub).getAggregations().get("vehicleBrandDistinct"))
                      .getValue());
        else if (name.equals("maxspeed"))
          System.out.print(
              " maxspeed: "
                  + ((ParsedMax) sub).getValue());
      }
      System.out.println("");
    }

    client.close();
  }
}
