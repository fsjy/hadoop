package com.yanglu.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

public class RpcStarter {

    public static void main(String[] args) throws IOException {


        RPC.Builder builder = new RPC.Builder(new Configuration());

        //
        builder.setBindAddress("192.168.1.100")         // 主机地址
                .setPort(10000)                         // 端口号
                .setProtocol(ILoginService.class)       // 接口
                .setInstance(new LoginServiceImpl());   // 实例

        Server server = builder.build();

        server.start();



    }
}
