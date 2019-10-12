package com.wxss.mq.Service.impl;

import com.rabbitmq.client.Channel;
import com.wxss.mq.Service.MessageSendService;
import com.wxss.mq.config.RabbitmqConfig;

/**
 * Author:Created by wx on 2019/10/12
 * Desc:
 */
public class MessageSendServiceImpl implements MessageSendService {
    public void sendMessage(String exchange, String queue, Object message) {

    }
}
