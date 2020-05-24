package com.li;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class TestNodeIndex {


    private static String host = "192.168.3.116";
    private static int port = 9300;
    TransportClient client;
    private static Settings.Builder settings = Settings.builder().put("cluster.name","my-application");
    @Before
    public void getClient() throws Exception {
        client = new PreBuiltTransportClient(settings.build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
    }

    @After
    public void close() {
        if (null != client) {
            client.close();
        }
    }

    /**
     * 创建索引添加文档
     * @throws Exception
     */
    @Test
    public void testIndex() throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","java编程思想");
        jsonObject.put("publishDate","2020.5.20");
        jsonObject.put("person","zhangsan");
        jsonObject.put("price",100);

        IndexResponse response = client.prepareIndex("book","java","1")
                .setSource(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引的名称:"+response.getIndex());
        System.out.println("类型:"+response.getType());
        System.out.println("文档ID:"+response.getId());
        System.out.println("当前实例状态:"+response.status());

    }

    @Test
    public void testUpdate() throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","java编程思想");
        jsonObject.put("publishDate","2020.5.20");
        jsonObject.put("person","zhangsan");
        jsonObject.put("price",102);

        UpdateResponse response = client.prepareUpdate("book","java","1")
                .setDoc(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引的名称:"+response.getIndex());
        System.out.println("类型:"+response.getType());
        System.out.println("文档ID:"+response.getId());
        System.out.println("当前实例状态:"+response.status());

    }

    @Test
    public void testGet()throws Exception {
        GetResponse response = client.prepareGet("book","java","1").get();
        System.out.println(response.getSourceAsString());
    }

    @Test
    public void testDelete()throws Exception {
        DeleteResponse response = client.prepareDelete("book","java","1").get();
        testGet();
        testIndex();
        testGet();
    }
}
