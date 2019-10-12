package direct;

import com.rabbitmq.client.*;
import com.wxss.mq.config.ConnectionFactoryConfig;
import com.wxss.mq.config.ExchangeConstant;
import com.wxss.mq.config.RabbitmqConfig;
import com.wxss.mq.entity.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import org.apache.commons.lang3.SerializationUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Author:Created by wx on 2019/10/12
 * Desc:
 */
public class DemoTests {

    private static Channel channel;

    @Before
    public void before() throws IOException, TimeoutException {
        ConnectionFactoryConfig connectionFactory = new ConnectionFactoryConfig();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("test-host1");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection = RabbitmqConfig.getConnection(connectionFactory);
        channel = RabbitmqConfig.getChannel(connection, null);
    }

    @After
    public void after() throws IOException, TimeoutException {
//        channel.close();
//        connection.close();
    }

    @Test
    public void sendStringMessage() {
        try {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare();
            // 声明交换机
            channel.exchangeDeclare(ExchangeConstant.DIRECT_EX_DEMO_ONE, BuiltinExchangeType.DIRECT);
            // 声明对列
            channel.queueDeclare("d1-queue", false, false, false, null);
            // 声明路由
            String routingKey = "demo1";
            // 消息
            byte[] messageBodyBytes = "hello,this is a message whose routingKey is demo1 and through by DIRECT_EX_DEMO_ONE".getBytes();
            // 发送
            channel.basicPublish(ExchangeConstant.DIRECT_EX_DEMO_ONE, routingKey, null, messageBodyBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void receiveStringMessage() throws IOException {
        // 声明对列
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare("d1-queue", false, false, false, null);
        String queue = declareOk.getQueue();
        // 声明交换机
        channel.exchangeDeclare(ExchangeConstant.DIRECT_EX_DEMO_ONE, BuiltinExchangeType.DIRECT);
        // 声明路由
        String bindingKey = "demo1";
        channel.queueBind(queue, ExchangeConstant.DIRECT_EX_DEMO_ONE, bindingKey);

        while (true) {
            channel.basicConsume(queue, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println(consumerTag);
                    System.out.println(envelope.getDeliveryTag());
                    System.out.println(envelope.getExchange());
                    System.out.println(envelope.getRoutingKey());

                    String s = new String(body, "UTF-8");
                    System.out.println(s);
                }
            });
        }
    }

    /**
     * 交换机没有绑定队列，消息默认会被丢弃
     */
    @Test
    public void sendObjectMessage() {
        try {
            // 声明交换机
            channel.exchangeDeclare(ExchangeConstant.DIRECT_EX_DEMO_TWO, BuiltinExchangeType.DIRECT);
            // 声明对列
            String queue = channel.queueDeclare("object-queue-demo2", false, false, false, null).getQueue();
            // 声明路由
            String routingKey = "object-demo2";
            channel.queueBind(queue, ExchangeConstant.DIRECT_EX_DEMO_TWO, routingKey);

            // 消息
            User user = new User();
            user.setUsername("测试员");
            user.setPassword("123456");
            user.setBirthday(new Date());
            Map<String,User> userMap = new HashMap<String, User>();
            userMap.put("A", user);

            byte[] body = map2Byte(userMap);
            // 发送
            channel.basicPublish(ExchangeConstant.DIRECT_EX_DEMO_TWO, routingKey, null, body);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 序列化
     * @param data
     * @return
     * @throws IOException
     */
    private byte[] map2Byte(Object data) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
        try {
            outputStream.writeObject(data);
        } finally {
            byteArrayOutputStream.close();
            outputStream.close();
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 反序列化化
     * @param data
     * @return
     * @throws IOException
     */
    private Object byte2Map(byte[] data) throws IOException {
        Object result = null;
        if (data == null || data.length == 0){
            return result;
        }
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            result = objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            objectInputStream.close();
        }
        return result;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 消息
        User user = new User();
        user.setUsername("测试员");
        user.setPassword("123456");
        user.setBirthday(new Date());
        Map<String,User> userMap = new HashMap<String, User>();
        userMap.put("A", user);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(baos);
        outputStream.writeObject(userMap);
        outputStream.close();
        byte[] bytes = baos.toByteArray();

        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
        Map<String,User> o = (Map<String, User>) objectInputStream.readObject();

        System.out.println(o);
    }

    @Test
    public void receiveObjectMessage() throws IOException {
        // 声明对列
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare("object-queue-demo2", false, false, false, null);
        String queue = declareOk.getQueue();
        // 声明绑定键
        String bindingKey = "object-demo2";
        // 交换器与队列绑定
        channel.queueBind(queue, ExchangeConstant.DIRECT_EX_DEMO_TWO, bindingKey);
        // 创建消费者
        System.out.println("receiveObjectMessage正在监听消息...");
        channel.basicConsume(queue, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                    super.handleDelivery(consumerTag, envelope, properties, body);
                System.out.println("receiveObjectMessage收到消息:");
//                User user = (User) SerializationUtils.deserialize(body);
//                System.out.println(user);
                Object deserialize = SerializationUtils.deserialize(body);
                Object o = byte2Map(body);
                System.out.println(deserialize);
                System.out.println(o);
            }
        });

        System.in.read();
    }

}
