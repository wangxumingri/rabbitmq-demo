package com.wxss.mq.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Author:Created by wx on 2019/10/12
 * Desc:
 */
public class RabbitmqConfig {

    private static ConnectionFactory connectionFactory ;

    public static Connection getConnection(ConnectionFactoryConfig connectionFactoryConfig) throws IOException, TimeoutException {
        if (connectionFactoryConfig == null){
            throw new RuntimeException("配置不存在，无法创建连接");
        }
        if (connectionFactory == null){
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(connectionFactoryConfig.getHost());
            connectionFactory.setPort(connectionFactoryConfig.getPort());
            connectionFactory.setVirtualHost(connectionFactoryConfig.getVirtualHost());
            connectionFactory.setUsername(connectionFactoryConfig.getUsername());
            connectionFactory.setPassword(connectionFactoryConfig.getPassword());
        }

        return connectionFactory.newConnection();
    }

    public static Channel getChannel(Connection connection,ChannelConfig channelConfig) throws IOException, TimeoutException {
        if (connection == null){
            throw new RuntimeException("连接不存在，无法创建channel");
        }
//        channel.confirmSelect();
//        channel.addConfirmListener(confirmListener);
        return connection.createChannel();
    }
}
