package com.itheima.es.test;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestElasticSearch {

    @Test
    //创建索引
    public void test01() throws IOException {
        //创建客端访问对象
        /**
         * Settings 表示集群的设置
         * EMPTY：表示没有集群的配置
         *
         */
        /**
         * 4.1.1.新建索引+添加文档
         使用创建索引（index）+类型（type）+自动创建映射（Elasticsearch帮助我们根据存储的字段自动建立映射，后续讲完分词器后，手动建立映射）
         */
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        //创建文档对象
        //方案一：组织Document数据
       /* Map<String, Object> map = new HashMap<>();
        map.put("id",3);
        map.put("title","3-ElasticSearch是一个基于Lucene的搜索服务器");
        map.put("content","3-它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。");
        map.put("time",new Date());*/

       //方案二：XContentBuilder
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject().field("id",4)
                .field("title","4ElasticSearch是一个基于Lucene的搜索服务器。")
                .field("content","4它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();
        //创建索引，创建文档类型，设置唯一主键。同时创建文档
        IndexResponse response = client.prepareIndex("blog","article","4")
                .setSource(xContentBuilder).get();//get()表示执行  == execute().actionGet();
        System.out.println("索引："+response.getIndex());
        System.out.println("类型："+response.getType());
        System.out.println("ID:"+response.getId());
        System.out.println("版本："+response.getVersion());
        //关闭资源
        client.close();

    }

    /**
     * 4.2.搜索文档数据
     4.2.1.ID查询（不走索引）
     */
    @Test
    public void queryById() throws Exception {

        /**
         * 创建Client：设置主机和端口
         * Settings.EMPTY非集群环境下的设置
         */
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        GetResponse getResponse = client.prepareGet("blog", "article", "1").get();
        System.out.println("json的字符串："+getResponse.getSourceAsString());
        //System.out.println("title字段的值："+getResponse.getSource().get("title"));
        // 关闭client
        client.close();
    }


    /**
     * 4.2.2.查询全部（不走索引查收）
     */
    @Test
    public void queryAll() throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        SearchResponse response = client.prepareSearch("blog").setTypes("article", "comment")
                .setQuery(QueryBuilders.matchAllQuery()) //查询所有
                .get();
        //返回结果集
        SearchHits searchHits = response.getHits();
        System.out.println("总记录数："+searchHits.getTotalHits());

        //for循环
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            System.out.println("json的字符串："+hit.getSourceAsString());
            System.out.println("title字段的值："+hit.getSource().get("title"));
            System.out.println("---------------------------------------------");
        }
        //关闭client
        client.close();

    }


    /**
     * 4.2.3.字符串查询:可以将搜索的内容分词再搜索,搜索完成后，再合并
     * 4.2.4.词条查询
     * 4.2.5.模糊查询（通配符查询）
     *：表示所有的任意的多个字符组成
     ?：表示1个任意的字符
     */
    @Test
    public void query() throws Exception {
        // 创建Client：指定主机和端口
//        Settings.EMPTY // 非集群环境下的设置
//        Settings.builder().put().build(); // 集群环境设置
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        SearchResponse response = client.prepareSearch("blog").setTypes("article", "comment")
               // .setQuery(QueryBuilders.matchAllQuery()) //查询所有
               // .setQuery(QueryBuilders.queryStringQuery("搜查").field("title")) //字符串查询，如果没有指定field，默认在所有的字段查询；如果指定field，就需要在指定的字段上查询
               // .setQuery(QueryBuilders.termQuery("title", "搜索")) //词条查询
                .setQuery(QueryBuilders.wildcardQuery("title", "*搜？*")) //模糊查询
                .get();

        // 返回结果集
        SearchHits searchHits = response.getHits();
        System.out.println("总记录数：" + searchHits.getTotalHits());
        // for循环
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            System.out.println("json的字符串：" + hit.getSourceAsString());
            System.out.println("title字段的值：" + hit.getSource().get("title"));
            System.out.println("_____________________________________________________________________");
        }
        // 关闭client
        client.close();
    }
}
