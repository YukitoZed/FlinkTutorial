package com.atguigu

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
object EsSink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromCollection(List(
      "a",
      "b"
    ))

    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("hadoop102",9200,"http"))

    val esSink: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](httpHosts,
      new ElasticsearchSinkFunction[String] {

        def createIndexRequest(element: String): IndexRequest = {
          val json: util.HashMap[String, String] = new util.HashMap[String, String]()
          json.put("data", element)

          Requests.indexRequest()
            .index("my-index")
            .`type`("my-type")
            .source(json)
        }

        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      })


    stream.addSink(esSink.build())

    env.execute()
  }
}
