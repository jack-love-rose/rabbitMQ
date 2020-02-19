package com.example.aurora.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.HashMap;
import java.util.Map;

/**
 * 开启ACK手动确认模式的配置类，这种java 配置类配置的就不用再application.yml中进行配置了
 * @ProjectName: rabbitmq
 * @Package: com.amor.config
 * @ClassName: RabbitmqConf
 * @Date: 2018/11/18 21:06
 * @Description:
 * @Version: 1.0
 */
//@Configuration
@Slf4j
public class RabbitmqConf {

    @Value("${spring.rabbitmq.host}")
    private String addresses;

    @Value("${spring.rabbitmq.port}")
    private int port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${spring.rabbitmq.virtual-host}")
    private String virtualHost;

    @Value("${spring.rabbitmq.publisher-confirms}")
    private boolean publisherConfirms;

    @Value("${spring.rabbitmq.publisher-returns}")
    private boolean publisherReturns;

    /**
     * 消息交换机的名字
     * */
    private static final String DIRECT_EXCHANGE = "DirectExchange";

    private static final String TOPIC_EXCHANGE = "TopicExchange";

    private static final String FANOUT_EXCHANGE ="FanoutExchange" ;

    private static final String HEADERS_EXCHANGE ="HeadersExchange" ;

    /**
     * 队列的名字
     * */
    private static final String DIRECT_QUEUE = "DirectQueue";

    private static final String TOPIC_QUEUE = "TopicQueue";

    private static final String FANOUT_QUEUE = "FanoutQueue";

    private static final String HEADERS_QUEUE = "HeadersQueue";

    /**
     * key
     * */
    private static final String DIRECT_KEY = "DirectKey";

    private static final String TOPIC_KEY = "Topic.#";



    /**
     * @Description: 连接工厂
     * @method: connectionFactory
     * @Param:
     * @return: org.springframework.amqp.rabbit.connection.ConnectionFactory
     * @auther: LHL
     * @Date: 2018/11/18 23:41
     */
    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(addresses,port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        /** 如果要进行消息发送确认回调，则这里必须要设置为true */
        connectionFactory.setPublisherConfirms(publisherConfirms);
        /** 如果要进行消息发送失败退回，则这里必须要设置为true */
        connectionFactory.setPublisherReturns(publisherReturns);
        return connectionFactory;
    }

    /**
     * 因为要设置回调类，所以应是prototype类型，如果是singleton类型，则回调类为最后一次设置
     * */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(connectionFactory());
        return template;
    }

    @Bean
    public SimpleMessageListenerContainer messageContainer() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
        /*container.setQueues(dirctQueue());*/
        container.setQueues(dirctQueue(),topicQueue(),fanoutQueue(),headersQueue());
        //将channel暴露给listener才能手动确认,AcknowledgeMode.MANUAL时必须为ture
        container.setExposeListenerChannel(true);
        //消费者的最大数量,并发消费的时候需要设置,且>=concurrentConsumers
        container.setMaxConcurrentConsumers(10);
        //消费者的最小数量
        container.setConcurrentConsumers(10);
        //在单个请求中处理的消息个数，他应该大于等于事务数量
        container.setPrefetchCount(1);
        //开启ACK  手动确认机制
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            try {
                // prefetchCount限制每个消费者在收到下一个确认回执前一次可以最大接受多少条消息,通过basic.qos方法设置prefetch_count=1,这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message
                channel.basicQos(1);
                log.info("ACK消费端接收到消息:" + message.getMessageProperties() + ":" + new String(message.getBody()));
                log.info("当前使用路由key:"+message.getMessageProperties().getReceivedRoutingKey());
                // deliveryTag：消息传送的次数,发布的每一条消息都会获得一个唯一的deliveryTag，(任何channel上发布的第一条消息的deliveryTag为1，此后的每一条消息都会加1)，deliveryTag在channel范围内是唯一的
                // multiple：批量确认标志。如果值为true，则执行批量确认，此deliveryTag之前收到的消息全部进行确认; 如果值为false，则只对当前收到的消息进行确认
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            } catch (Exception e) {
                e.printStackTrace();
                if (message.getMessageProperties().getRedelivered()) {
                    log.info("消息已重复处理失败,拒绝再次接收...");
                    // deliveryTag：消息传送的次数,发布的每一条消息都会获得一个唯一的deliveryTag，deliveryTag在channel范围内是唯一的
                    // multiple：批量确认标志。如果值为true，包含本条消息在内的、所有比该消息deliveryTag值小的 消息都被拒绝了（除了已经被 ack 的以外）;如果值为false，只拒绝三本条消息
                    // requeue：如果值为true，则重新放入RabbitMQ的发送队列，如果值为false，则通知RabbitMQ销毁这条消息
                    channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
                } else {
                    log.info("消息即将再次返回队列处理...");
                    // deliveryTag：消息传送的次数,发布的每一条消息都会获得一个唯一的deliveryTag，deliveryTag在channel范围内是唯一的
                    // requeue：如果值为true，则重新放入RabbitMQ的发送队列，如果值为false，则通知RabbitMQ销毁这条消息
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                }
            }
        });
        return container;
    }


    /**
     * 1.队列名字
     * 2.durable="true" 是否持久化 rabbitmq重启的时候不需要创建新的队列
     * 3.auto-delete    表示消息队列没有在使用时将被自动删除 默认是false
     * 4.exclusive      表示该消息队列是否只在当前connection生效,默认是false
     */
    @Bean
    public Queue dirctQueue() {
        return new Queue(DIRECT_QUEUE,true,false,false);
    }

    @Bean
    public Queue topicQueue() {
        return new Queue(TOPIC_QUEUE,true,false,false);
    }

    @Bean
    public Queue fanoutQueue() {
        return new Queue(FANOUT_QUEUE,true,false,false);
    }

    @Bean
    public Queue headersQueue() {
        return new Queue(HEADERS_QUEUE,true,false,false);
    }

    /**
     * 1.交换机名字
     * 2.durable="true" 是否持久化 rabbitmq重启的时候不需要创建新的交换机
     * 3.autoDelete    当所有消费客户端连接断开后，是否自动删除队列
     */
    @Bean
    public DirectExchange directExchange(){
        return new DirectExchange(DIRECT_EXCHANGE,true,false);
    }

    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(TOPIC_EXCHANGE,true,false);
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE,true,false);
    }

    @Bean
    public HeadersExchange headersExchange() {
        return new HeadersExchange(HEADERS_EXCHANGE,true,false);
    }

    /**
     * 将direct队列和交换机进行绑定
     */
    @Bean
    public Binding bindingDirect() {
        return BindingBuilder.bind(dirctQueue()).to(directExchange()).with(DIRECT_KEY);
    }

    @Bean
    public Binding bindingTopic() {
        return BindingBuilder.bind(topicQueue()).to(topicExchange()).with(TOPIC_KEY);
    }


    @Bean
    public Binding bindingFanout() {
        return BindingBuilder.bind(fanoutQueue()).to(fanoutExchange());
    }

    @Bean
    public Binding headersBinding(){
        Map<String,Object> map = new HashMap<>();
        map.put("headers1","value1");
        map.put("headers2","value2");
        return BindingBuilder.bind(headersQueue()).to(headersExchange()).whereAll(map).match();
    }

    /**
     * 定义消息转换实例  转化成 JSON 传输  传输实体就可以不用实现序列化
     * */
    @Bean
    public MessageConverter integrationEventMessageConverter() {
        return  new Jackson2JsonMessageConverter();
    }
}
