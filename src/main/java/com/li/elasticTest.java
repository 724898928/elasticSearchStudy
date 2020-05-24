package com.li;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class elasticTest {
    private static String host = "192.168.3.115";
    private static int port = 9300;

    public static void main(String[] args) {
        try {
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
            System.out.println(client);
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

}
