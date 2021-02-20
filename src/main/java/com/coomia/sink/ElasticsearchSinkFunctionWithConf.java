/*******************************************************************************
 * /*
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  */
package com.coomia.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;

public class ElasticsearchSinkFunctionWithConf
    implements ElasticsearchSinkFunction<String>, Serializable {

  private static final long serialVersionUID = 1L;
  private String index;

  public ElasticsearchSinkFunctionWithConf(String index) {
    this.index = index;
  }

  @Override
  public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
    indexer.add(Requests.indexRequest().index(index).source(element, XContentType.JSON));
  }
}