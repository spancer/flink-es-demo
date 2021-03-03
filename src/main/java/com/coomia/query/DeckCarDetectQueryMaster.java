/**
 * Project Name:flink-es-sink-demo File Name:DeckCarDetectQuery.java Package Name:com.coomia.query
 * Date:2020年9月15日上午9:02:49 Copyright (c) 2020, spancer.ray All Rights Reserved.
 */
package com.coomia.query;

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
import org.elasticsearch.plugin.maxspeed.MaxspeedAggregationBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ClassName:DeckCarDetectQuery Function: 通过套牌车辆分析，可及时掌握套牌情况，加强对公共安全的管控。
 * 不同车身、不同车型、不同品牌、不同区域(deviceID)同时出现(within time interval) Date: 2020年9月15日 上午9:02:49
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class DeckCarDetectQueryMaster {

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
    CardinalityAggregationBuilder color =
        AggregationBuilders.cardinality("plateColorDescDistinct").field("plateColorDesc");
    CardinalityAggregationBuilder clas =
        AggregationBuilders.cardinality("vehicleClassDescDistinct").field("vehicleClassDesc");
    CardinalityAggregationBuilder brand =
        AggregationBuilders.cardinality("vehicleBrandDistinct").field("VehicleBrand");
    List fields = new ArrayList<String>();
    fields.add("shotTime");
    fields.add("location");
    MaxspeedAggregationBuilder maxSpeedAgg =
        new MaxspeedAggregationBuilder("maxspeed").fields(fields);

    /** build script and params. */
    Map<String, String> bucketsPathsMap = new HashMap<String, String>();
    bucketsPathsMap.put("plateColorDescDistinct", "plateColorDescDistinct");
    bucketsPathsMap.put("vehicleClassDescDistinct", "vehicleClassDescDistinct");
    bucketsPathsMap.put("vehicleBrandDistinct", "vehicleBrandDistinct");
    bucketsPathsMap.put("maxspeed", "maxspeed");
    Map<String, Object> havingScriptParam = new HashMap<String, Object>();
    havingScriptParam.put("havingCount", 1); // distinct count > 1，即表示有重复的数据（不同颜色或不同型号或其它）
    havingScriptParam.put("speed", 120d); //
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
            .subAggregation(maxSpeedAgg)
            .subAggregation(having));
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
    List<? extends org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> data =
        devices.getBuckets();
    for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bks : data) {
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
              + ((ParsedCardinality) bks.getAggregations().get("vehicleBrandDistinct")).getValue()
              + " SpeedMetrics  "
              + ((ParsedMax) bks.getAggregations().get("maxspeed")).getValue());
    }

    client.close();
  }
}
