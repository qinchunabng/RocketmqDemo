package com.qin.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 同步生产者
 *
 * @author qcb
 * @date 2022/07/10 15:35.
 */
public class SyncProducer {

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
            //创建消息实例，定义topic、tag和消息体
            Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //发送消息
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
            TimeUnit.SECONDS.sleep(5);
        }
        //关闭producer
        producer.shutdown();
    }
}
