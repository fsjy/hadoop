package com.yanglu.hadoop.rpc.controller;

import com.yanglu.hadoop.rpc.ILoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class LoginController {

    public static void main(String[] args) throws IOException {
        ILoginService proxy = RPC.getProxy(ILoginService.class,
                1,
                new InetSocketAddress("192.168.1.100", 10000), new Configuration()
        );

        String result = proxy.login("lulu", "123456");

        System.out.println(result);

    }
}
