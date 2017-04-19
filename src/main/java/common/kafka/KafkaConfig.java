
package common.kafka;

/*
 * Created by cloud computing on 2016/9/21 0021.
 */
public interface KafkaConfig {

    /**
     * DEFAULT_BROKERS
     */
    public static final String DEFAULT_BROKERS = "localhost:9092";
    /**
     * DEFAULT_TYPE
     */
    public static final String DEFAULT_TYPE = "sync";
    /**
     * DEFAULT_ACKS
     */
    public static final String DEFAULT_ACKS = "0";
    /**
     * DEFAULT_CODEC
     */
    public static final String DEFAULT_CODEC = "none";
    /**
     * DEFAULT_BATCH
     */
    public static final String DEFAULT_BATCH = "200";

    /**
     * BROKERS_LIST_PROPERTY
     */
    public static final String BROKERS_LIST_PROPERTY = "metadata.broker.list";
    public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
    /**
     * PRODUCER_TYPE_PROPERTY
     */
    public static final String PRODUCER_TYPE_PROPERTY = "producer.type";
    /**
     * REQUEST_ACKS_PROPERTY
     */
    public static final String REQUEST_ACKS_PROPERTY = "request.required.acks";
    /**
     * COMPRESSION_CODEC_PROPERTY
     */
    public static final String COMPRESSION_CODEC_PROPERTY = "compression.codec";
    /**
     * BATCH_NUMBER_PROPERTY
     */
    public static final String BATCH_NUMBER_PROPERTY = "batch.num.messages";

    /**
     * @since 1.2.3
     */
    public static final String BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers";
    public static final String KEY_SERIALIZER_PROPERTY = "key.serializer";
    public static final String VAL_SERIALIZER_PROPERTY = "value.serializer";
    public static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

}
