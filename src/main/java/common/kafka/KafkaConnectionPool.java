
package common.kafka;



import common.ConnectionPool;
import common.PoolBase;
import common.PoolConfig;
import org.apache.kafka.clients.producer.Producer;


import java.util.Properties;

/*
 * Created by cloud computing on 2016/9/21 0021.
 */
public class KafkaConnectionPool extends PoolBase<Producer<byte[], byte[]>> implements ConnectionPool<Producer<byte[], byte[]>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -1506435964498488591L;

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     */
    public KafkaConnectionPool() {

        this(KafkaConfig.DEFAULT_BROKERS);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param brokers broker
     */
    public KafkaConnectionPool(final String brokers) {

        this(new PoolConfig(), brokers);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param props
     */
    public KafkaConnectionPool(final Properties props) {

        this(new PoolConfig(), props);
    }


    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param poolConfig
     * @param brokers    broker
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers) {

        this(poolConfig, brokers, KafkaConfig.DEFAULT_TYPE, KafkaConfig.DEFAULT_ACKS, KafkaConfig.DEFAULT_CODEC, KafkaConfig.DEFAULT_BATCH);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param poolConfig
     * @param brokers    broker
     * @param type
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers, final String type) {

        this(poolConfig, brokers, type, KafkaConfig.DEFAULT_ACKS, KafkaConfig.DEFAULT_CODEC, KafkaConfig.DEFAULT_BATCH);
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param poolConfig
     * @param config
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final Properties config) {

        super(poolConfig, new KafkaConnectionFactory(config));
    }

    /**
     * <p>Title: KafkaConnectionPool</p>
     * <p>Description: </p>
     *
     * @param poolConfig
     * @param brokers    broker
     * @param type
     * @param acks
     * @param codec
     * @param batch
     */
    public KafkaConnectionPool(final PoolConfig poolConfig, final String brokers, final String type, final String acks, final String codec, final String batch) {

        super(poolConfig, new KafkaConnectionFactory(brokers, type, acks, codec, batch));
    }

    @Override
    public Producer<byte[], byte[]> getConnection() {

        return super.getResource();
    }

    @Override
    public void returnConnection(Producer<byte[], byte[]> conn) {

        super.returnResource(conn);
    }

    @Override
    public void invalidateConnection(Producer<byte[], byte[]> conn) {

        super.invalidateResource(conn);
    }
}
