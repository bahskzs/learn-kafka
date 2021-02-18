package com.yqy.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * @author bahsk
 * @createTime 2021-01-31 22:50
 * @description
 */
public class AdminTest {

    Admin adminClient = null;
    public final static String TOPIC_NAME = "test";

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"master610:9092");
        adminClient = AdminClient.create(properties);
    }

    @Test
    public void testAdminClient(){

        System.out.println(adminClient);
    }

     /**
      * @author: bahsk
      * @date: 2021/2/18 21:49
      * @description: 创建topic
      * @params:
      * @return:
      */
    @Test
    public void testCreateNewTopic() {
        //String topic = "test-topic";
        //副本数量--跟hdfs类似的副本数
        Short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME,1, replicationFactor);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        Assert.assertNotNull(createTopicsResult);

    }

    @Test
    public void listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNames = listTopicsResult.names().get();
        //打印
        //System.out::println是Consumer<T>接口的一个实现方式
        topicNames.stream().forEach(System.out::println);

    }


     /**
      * @author: bahsk
      * @date: 2021/2/18 22:03
      * @description: 打印内部topics
      * @params:
      * @return:
      */
    @Test
    public void listInternalTopics() throws ExecutionException, InterruptedException {

        //
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);


        //获取topic list的名称
        Set<String> topicNames = listTopicsResult.names().get();
        //打印
        //System.out::println是Consumer<T>接口的一个实现方式
        topicNames.stream().forEach(System.out::println);

        Collection<TopicListing> topicsList = listTopicsResult.listings().get();
        //打印topicListing
        topicsList.stream().forEach(topicListing -> {
            System.out.println(topicListing.name());
        });

        Map<String, TopicListing> mapKafkaFuture = listTopicsResult.namesToListings().get();

    }

     /**
      * @author: bahsk
      * @date: 2021/2/18 22:26
      * @description: 删除topic ,deleteTopicsResult.all().get();
      * @params:
      * @return:
      */
    @Test
    public void testDeleteTopic() throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
        Assert.assertNotNull(deleteTopicsResult);
    }


     /**
      * @author: bahsk
      * @date: 2021/2/18 22:26
      * @description: entrySet(),函数式编程
      * @params:
      * @return: name:test,
      * desc(
      *     name=test, internal=false,
      *     partitions=(
      *                 partition=0,
      *                 leader=master610:9092 (id: 0 rack: null),
      *                 replicas=master610:9092 (id: 0 rack: null),
      *                 isr=master610:9092 (id: 0 rack: null)
      *                 ),
      *     authorizedOperations=null
      *     )
      */
    @Test
    public void testDescribeTopic() throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        //有get到集合的操作方式
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = topicDescriptionMap.entrySet();
        entries.stream().forEach(stringTopicDescriptionEntry -> {
            System.out.println("name:" + stringTopicDescriptionEntry.getKey() + ", desc:" + stringTopicDescriptionEntry.getValue());
        });
    }

     /**
      * @author: bahsk
      * @date: 2021/2/18 22:34
      * @description: 查看topic的配置信息
      * @params:
      * @return: ConfigResource: ConfigResource(type=TOPIC, name='test'),
      * config: Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=1.0-IV0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
      * ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
      */
    @Test
    public void testDescribeConfigs() throws ExecutionException, InterruptedException {
        //
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        Set<Map.Entry<ConfigResource, Config>> entries = configResourceConfigMap.entrySet();
        entries.stream().forEach(configResourceConfigEntry -> {
            System.out.println("ConfigResource: " + configResourceConfigEntry.getKey() + ", config: " + configResourceConfigEntry.getValue());
        });
    }

     /**
      * @author: bahsk
      * @date: 2021/2/18 23:06
      * @description: 修改Topic配置
      * @params:
      * @return:
      */
    @Test
    public void  testAlterConfig() throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        /*2.3 以后的写法，由于我的服务端还是1.0
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate","true"), AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(configResource,Arrays.asList(alterConfigOp));
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configs);
        alterConfigsResult.all().get();
        */
        Map<ConfigResource, Config> configs = new HashMap<>();
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate","true")));
        configs.put(configResource,config);

        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configs);
        alterConfigsResult.all().get();
    }

     /**
      * @author: bahsk
      * @date: 2021/2/18 23:13
      * @description: 增加分区
      * @params:
      * @return:
      */
    @Test
    public void increPartitions() throws ExecutionException, InterruptedException {

        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();

        NewPartitions newPartitions = NewPartitions.increaseTo(2);

        newPartitionsMap.put(TOPIC_NAME,newPartitions);

        CreatePartitionsResult partitions = adminClient.createPartitions(newPartitionsMap);
        partitions.all().get();
    }

    @After
    public void tearDown() {
        adminClient.close();
    }
}
