package com.qin.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * One-way 模式
 *
 * @author qcb
 * @date 2022/07/10 16:31.
 */
public class OnewayProducer {

    public static void main(String[] args) throws Exception{
        //实例化生产者，并定义一个groupName
        DefaultMQProducer producer = new DefaultMQProducer("sync_producer_group");
        //设置name server地址
        producer.setNamesrvAddr("192.168.3.99:9876;192.168.3.100:9876");
        //设置发送超时时间
        producer.setSendMsgTimeout(60000);
        //启动生产者
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message("TopicTest", "TagB", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(message);
            TimeUnit.SECONDS.sleep(2);
        }
        TimeUnit.MINUTES.sleep(5);
        producer.shutdown();
    }
}
