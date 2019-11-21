package com.common

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
object Sink2ES {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.fromCollection(List("心态","爆炸"))

    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop102",9200,"http"))

    val esSink: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](
      hosts,
      new ElasticsearchSinkFunction[String] {

        def createIndexRequest(t: String): IndexRequest = {
          val json: util.HashMap[String, String] = new util.HashMap[String, String]()
          json.put("data", t)

          Requests.indexRequest()
            .index("my-index")
            .`type`("my-type")
            .source(json)
        }

        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      })

    stream.addSink(esSink.build)

    env.execute()
  }

}
