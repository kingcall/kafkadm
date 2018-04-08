package topic;

import kafka.admin.AdminUtils;
import kafka.common.Config;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.ZKUtil;

import java.util.Properties;

/**
 * @program: kafkadm
 * @description: java对kafka Topic的操作
 * @author: 刘文强  kingcall
 * @create: 2018-04-08 23:14
 *
 * 问题是 zookeeper 提供的zkutil 和 kafka 提供的 zkutils 的关系
 **/
public class Topics {
    public static  final String ZK_CONNECT="master:2181";
    public static  final int SESSION_TIMEOUT=1000;
    public static  final int CONNECT_TIMEOUT=1000;

    public static void main(String[] args) {
        Properties properties=new Properties();
        //CreateTopic("API",6,3,properties);
        modifyTopicConfig("API");
    }
    public static void CreateTopic(String topic, int partitios, int replica, Properties properties){
        ZkUtils zkUtils=null;
        zkUtils=ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
        if (!AdminUtils.topicExists(zkUtils,topic)){
            AdminUtils.createTopic(zkUtils,topic,partitios,replica,properties,AdminUtils.createTopic$default$6());
        }else {
            System.out.println("TOPIC:"+topic+" has been existed");
        }
    }
    public static void modifyTopicConfig(String topic){
        ZkUtils zkUtils=null;
        zkUtils=ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
        Properties curProp=AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(),topic);
        System.out.println(curProp);
        Properties addprop=new Properties();
        addprop.put("segment.bytes",200000000);
        AdminUtils.changeTopicConfig(zkUtils,topic,addprop);

    }
}
