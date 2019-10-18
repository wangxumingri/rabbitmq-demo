package direct;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Author:Created by wx on 2019/10/12
 * Desc:
 */
public class DemoTests {

    private static Channel channel;

//    @Before
//    public void before() throws IOException, TimeoutException {
//        ConnectionFactoryConfig connectionFactory = new ConnectionFactoryConfig();
//        connectionFactory.setHost("localhost");
//        connectionFactory.setPort(5672);
//        connectionFactory.setVirtualHost("test2");
//        connectionFactory.setUsername("guest");
//        connectionFactory.setPassword("guest");
//
//        Connection connection = RabbitmqConfig.getConnection(connectionFactory);
//        channel = RabbitmqConfig.getChannel(connection, null);
//    }

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

    /**
     * 测试BasicPublish的mandatory属性，未绑定队列
     *  mandatory：
     *      true:监听会被执行
     *      false:监听不会被执行，消息直接被broker丢弃
     */
    @Test
    public void testBasicPublishWithMandatoryUnBind() throws IOException {
        String queue = "Test-Mandatory-Queue-UnBind";
        String exchange = "Test-Mandatory-Exchange-UnBind";
        String error_RoutingKey = "Error_RoutingKey";
        String correct_RoutinKey = "Correct_RoutingKey";
        String default_RoutingKey = "";

        channel.basicQos(1);
        // 监听
        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                System.out.println("testBasicPublishWithMandatoryUnBind:Basic.return返回的结果是："+message);
            }
        });
        // 声明消息队列
        channel.queueDeclare(queue, false, false, true, null);
        // 创建交换机
        channel.exchangeDeclare(exchange,BuiltinExchangeType.DIRECT );
        // 发布消息，并设mandatory为true,且该交换机未绑定队列,
        channel.basicPublish(exchange,correct_RoutinKey , true, MessageProperties.TEXT_PLAIN, "测试mandatory".getBytes());
    }


    /**
     * 测试BasicPublish的mandatory属性，已绑定队列,且routingKey正确
     *  mandatory:
     *      消息正常投递到queue
     */
    @Test
    public void testBasicPublishWithMandatoryBind() throws IOException {
        String queue = "Test-Mandatory-Queue-Bind";
        String exchange = "Test-Mandatory-Exchange-Bind";
        String error_RoutingKey = "Error_RoutingKey";
        String correct_RoutinKey = "Correct_RoutingKey";
        String default_RoutingKey = "";

        channel.basicQos(1);
        // 监听
        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                String message = new String(body);
                System.out.println(replyCode+"\n"+replyText);
                System.out.println("testBasicPublishWithMandatoryBind:Basic.return返回的结果是："+routingKey);
            }
        });
        // 声明消息队列
        channel.queueDeclare(queue, false, false, true, null);
        // 创建交换机
        channel.exchangeDeclare(exchange,BuiltinExchangeType.DIRECT );
        // 绑定队列
        channel.queueBind(queue, exchange, correct_RoutinKey);
        // 发布消息，并设mandatory为true,且该交换机绑定队列,
        channel.basicPublish(exchange,correct_RoutinKey, true, MessageProperties.TEXT_PLAIN, "测试mandatory:set mandatory value as true and bind queue to exchange with routingKey".getBytes());
    }

    @Test
    public void testQueueDeclareWithExclusive() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri("amqp://guest:guest@localhost:5672/test2");
        Connection connection_1 = connectionFactory.newConnection();
        // 在连接1下创建一个channel : channel_1
        Channel channel_1 = connection_1.createChannel();
        // 使用channel_1创建一个独占队列
        AMQP.Queue.DeclareOk queue1 = channel_1.queueDeclare("独占队列1", true, true, false, null);
        System.out.println(queue1.getQueue());
        channel_1.basicPublish("", "独占队列1", null,"独占队列1".getBytes());
        // 关闭channel_1
        channel_1.close();
        System.out.println("--------------");
        // 在连接1下，再创建一个channel:channel_2
        Channel channel_2 = connection_1.createChannel();
        // 使用channel_2获取同一连接下另一个channel创建的独占队列
        AMQP.Queue.DeclareOk declareOk = channel_2.queueDeclarePassive(queue1.getQueue());
        System.out.println(declareOk.getQueue()); // 会输出:独占队列1
        // 关闭连接1
//        connection_1.close();
        System.out.println("--------------");
        // 新建一个连接2
        Connection connection_2 = connectionFactory.newConnection();
        // 连接2创建一个channel去获取连接1下的独占队列
        Channel channelOf2 = connection_2.createChannel();
        /**
         * 连接1关闭时：会报404 (reply-code=404, reply-text=NOT_FOUND - no queue '独占队列1' in vhost 'test2'.
         * 连接1未关闭时：405 (reply-code=405, reply-text=RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '独占队列1' in vhost 'test2'.
         *      It could be originally declared on another connection or the exclusive property value does not match that of the original declaration., class-id=50, method-id=10)
         */
        AMQP.Queue.DeclareOk declareOk1 = channelOf2.queueDeclarePassive(queue1.getQueue());
    }

    @Test
    public void testGson(){
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").setPrettyPrinting();
        Gson gson = builder.create();
        List<User> userList = new ArrayList<>();
        User user = new User();
        user.setUsername("测试1");
        user.setPassword("123");
        user.setBirthday(new Date());

        userList.add(user);

        user = new User();
        user.setUsername("测试2");
        user.setPassword("456");
        user.setBirthday(new Date());

        userList.add(user);

        String json = gson.toJson(userList);
        System.out.println(json);

        List<User> list = gson.fromJson(json, TypeToken.getParameterized(List.class,User.class).getType());

        System.out.println(list);

    }
}
