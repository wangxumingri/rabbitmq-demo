package direct;

import com.rabbitmq.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Author:Created by wx on 2019/10/17
 * Desc:
 */
public class Demo2Test {

    private static Channel channel;

    @Before
    public void before() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://guest:guest@localhost:5672/test2");
        System.out.println(factory.getConnectionTimeout());
        factory.setConnectionTimeout(0);
        System.out.println(factory.getConnectionTimeout());
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        System.out.println(channel.getChannelNumber());
        System.out.println("before:...........");
    }

    /**
     *  测试声明队列或者交换机自动删除属性
     *      队列：autoDelete为true时，至少有一个消费者连接到这个队列，之后所有与这个队列连接的消费者都断开后，才会自动删除
     *      交换器：autoDelete为true时，只有有一个队列或者交换器绑定到这个交换器，之后所有与这个交换器绑定的队列或者交换器解绑后，才会自动删除
     */
    @Test
    public void testAutoDelete() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        AMQP.Queue.DeclareOk auto_queue = channel.queueDeclare("auto_queue", false, false, true, null);
        String queueName = auto_queue.getQueue();
        channel.exchangeDeclare("auto_ex", BuiltinExchangeType.DIRECT, false, true, null);
        channel.basicPublish("auto_ex", "auto",MessageProperties.TEXT_PLAIN, "测试".getBytes());
//        channel.queueBind(queueName, "auto_ex", "auto");
    }

    @Test
    public void testMandatory() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("mandatory：无法将消息路由到队列:");
                System.out.println("replyCode="+replyCode+";replyText="+replyText+";body="+new String(body,"UTF-8"));
            }
        });
        channel.queueDeclare("mandatory_queue", false, false, true, null);
        channel.exchangeDeclare("mandatory_ex", BuiltinExchangeType.DIRECT, false, true, null);
//        channel.queueBind(queueName, "auto_ex", "auto");
        for (int i = 0; i < 20; i++) {
            channel.basicPublish("mandatory_ex", "aasfaf",true,MessageProperties.TEXT_PLAIN, ("测试"+i).getBytes());
        }
        System.in.read();
    }

    /**
     * RabbitMQ 3.0+版本取消对immediate参数的支持，设为true时，会报错
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws URISyntaxException
     * @throws IOException
     * @throws TimeoutException
     */
    @Test
    public void testImmediate() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("mandatory：无法将消息路由到队列:");
                System.out.println("replyCode="+replyCode+";replyText="+replyText+";body="+new String(body,"UTF-8"));
            }
        });
        channel.queueDeclare("immediate_queue", false, false, true, null);
        channel.exchangeDeclare("immediate_ex", BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueBind("immediate_queue", "immediate_ex", "immediate_Key");
//        channel.basicPublish("immediate_ex", "immediate_Key", null, "aa".getBytes());

//        for (int i = 0; i < 3; i++) {
//            channel.basicPublish("immediate_ex", "immediate_Key",true,true,MessageProperties.TEXT_PLAIN, ("测试"+i).getBytes());
            channel.basicPublish("immediate_ex", "immediate_Key",false,true,MessageProperties.TEXT_PLAIN, ("测试").getBytes());
//        }

        System.in.read();
    }

    @Test
    public void testAutoDeleteConsumer() throws IOException {

        Consumer consumer = new DefaultConsumer(channel){

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("收到消息");
                System.out.println(consumerTag);
                System.out.println(new String(body));
            }
        };
        channel.basicConsume("auto_queue",consumer );
    }

    /**
     * 测试备份交换器:
     *  声明一个正常交换器时，设置其alternate-exchange属性，指定该交换器消息路由失败时，将消息转发到备份交换器
     */
    @Test
    public void testAlternateExchange() throws IOException {
        Map<String,Object> args = new HashMap<>();
        args.put("alternate-exchange","alternate_Ex");
        // 声明一个正常的队列，并设置属性alternate-exchange
        channel.exchangeDeclare("normal_Ex", BuiltinExchangeType.DIRECT, false, true,args);
        // 声明一个备份交换器
        channel.exchangeDeclare("alternate_Ex", BuiltinExchangeType.FANOUT, false, true, null);

        // 声明一个的队列，并绑定到正常交换器
        channel.queueDeclare("normal_queue", false,false, true, null);
        channel.queueBind("normal_queue", "normal_Ex", "normal");

        // 声明一个的队列，并绑定到备份交换器
        channel.queueDeclare("alternate_queue", false,false, true, null);
        channel.queueBind("alternate_queue", "alternate_Ex", ""); // fanout，routingKey没用

        // 发送消息：
        // 正常路由
//        channel.basicPublish("normal_Ex", "normal", null, "消息".getBytes());
        // 路由失败
        channel.basicPublish("normal_Ex", "nonexistentRoutingKey", null, "消息".getBytes());
    }


    /**
     *  mandatory个备份交换器一起使用时，mandatory参数会失效
     * @throws IOException
     */
    @Test
    public void testMandatoryAndAlternateExchange() throws IOException {
        // 返回监听器
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("ReturnListener 运行:"+replyText);
            }
        });
        // 路由失败
        channel.basicPublish("normal_Ex", "nonexistentRoutingKey", true,null, "测试mandatory个备份交换器一起使用".getBytes());
    }


    /**
     * 测试消息过期时间：时间到后，消息自动过期
     *   如果使用默认交换机，则basicPublish的exchange="",routingKey="要路由到的队列名称"
     *      1.在声明队列时，设置其x-message-ttl属性，时间单位为毫秒，该队列中所有消息ttl一致
     *      2.在发送消息时，为每个消息分别设置ttl
     */
    @Test
    public void testMessageTTL() throws IOException {
//         声明一个队列，并设置该队列ttl属性为12秒
        Map<String,Object> args = new HashMap<>();
        args.put("x-message-ttl",12000);
        channel.queueDeclare("ttl_queue", false, false,true, args );
        channel.basicPublish("", "ttl_queue", null, "使用默认交换器测试消息过期时间ttl".getBytes());
        channel.queueDelete("ttl_queue");
        // 重新声明一个普通队列
        channel.queueDeclare("ttl_queue", false, false,true, null);
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
        builder.expiration("12000");
        channel.basicPublish("", "ttl_queue",builder.build() , "为每个消息单独设置过期时间ttl".getBytes());
    }

    /**
     * 测试队列的TTL
     *    到期自动删除未使用的队列
     *   未使用：
     *      1.没有消费者
     *      2.在一个周期内，该队列也没有重新被声明
     *      3.在一个周期内，没有调用Basic.Get命令
     */
    @Test
    public void testQueueTTL() throws IOException {
        Map<String,Object> args = new HashMap<>();
        args.put("x-expires", 60000*3);
        channel.queueDeclare("queue_ttl", false, false, true, args);
    }

    @Test
    public void testBig(){
//        double d1 = 2.31;
//        double d2 = 2.0;
//        double v = d1 * d2;
//        System.out.println(v);
//        BigDecimal b1 = new BigDecimal(2.6D);
//        System.out.println(b1);
//        BigDecimal b2 = new BigDecimal(2.40D);
//        System.out.println(b2);
//        BigDecimal add = b1.add(b2);
//        BigDecimal multiply = b1.multiply(b2);
//
//        System.out.println(add);
//        System.out.println("-------------");
//        BigDecimal b3 = new BigDecimal("2.6");
//        BigDecimal b4 = new BigDecimal("2.40");
//        BigDecimal add1 = b3.add(b4);
//        BigDecimal multiply1 = b3.multiply(b4);
//        System.out.println("--------------");
//        BigDecimal multiply2 = b1.multiply(b4);

        BigDecimal b1 = new BigDecimal("2.1356");
        BigDecimal b2 = new BigDecimal("2.0501300");
        BigDecimal b3 = new BigDecimal("2.000");
        BigDecimal b4 = new BigDecimal("2.55");

        BigDecimal decimal3 = b4.setScale(1, RoundingMode.HALF_UP);
        long l = decimal3.longValue();
        decimal3 = b4.setScale(1,RoundingMode.HALF_DOWN);
        long l2 = decimal3.longValue();



        BigDecimal decimal = b1.setScale(2, RoundingMode.HALF_UP);
        BigDecimal decimal1 = b2.setScale(5,RoundingMode.HALF_UP);
        BigDecimal decimal2 = b3.setScale(2,RoundingMode.HALF_UP);
        double v = b3.doubleValue();
        String s = b3.toPlainString();
        String s1 = b3.toString();

        double v1 = b2.doubleValue();


        BigDecimal decimal222 = b1.setScale(2, RoundingMode.HALF_DOWN);
        BigDecimal decimal122 = b2.setScale(5,RoundingMode.HALF_DOWN);
        BigDecimal decimal22 = b3.setScale(2,RoundingMode.HALF_DOWN);


        BigDecimal decimal4 = new BigDecimal("2.1654600", new MathContext(3, RoundingMode.HALF_DOWN));

        BigDecimal decimal5 = new BigDecimal("2.165").setScale(2, RoundingMode.HALF_DOWN);
        BigDecimal decimal6 = new BigDecimal("2.165").setScale(2, RoundingMode.HALF_UP);

    }
}
