package com.wxss.mq.Service;

/**
 * Author:Created by wx on 2019/10/12
 * Desc:
 */
public interface MessageSendService {
    void sendMessage(String exchange,String queue,Object message);
}
