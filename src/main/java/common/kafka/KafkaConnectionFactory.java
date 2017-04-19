
package common.kafka;



import common.ConnectionException;
import common.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;


import java.util.Properties;

/*
 * Created by cloud computing on 2016/9/21 0021.
 */
class KafkaConnectionFactory implements ConnectionFactory<Producer<byte[], byte[]>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 8271607366818512399L;

    /**
     * config
     */
    private final Properties config;


    /**
     * <p>Title: KafkaConnectionFactory</p>
     * <p>Description: </p>
     *
     * @param brokers broker
     * @param type
     * @param acks
     * @param codec
     * @param batch
     */
    public KafkaConnectionFactory(final String brokers, final String type, final String acks, final String codec, final String batch) {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKERS_LIST_PROPERTY, brokers);
        props.setProperty(KafkaConfig.PRODUCER_TYPE_PROPERTY, type);
        props.setProperty(KafkaConfig.REQUEST_ACKS_PROPERTY, acks);
        props.setProperty(KafkaConfig.COMPRESSION_CODEC_PROPERTY, codec);
        props.setProperty(KafkaConfig.BATCH_NUMBER_PROPERTY, batch);
        this.config = props;
    }

    /**
     * @param properties
     * @since 1.2.1
     */
    public KafkaConnectionFactory(final Properties properties) {

        String brokersA = properties.getProperty(KafkaConfig.BROKERS_LIST_PROPERTY);
        String brokersB = properties.getProperty(KafkaConfig.BOOTSTRAP_SERVER);
        if (brokersA == null && brokersB == null)
            throw new ConnectionException("[" + KafkaConfig.BROKERS_LIST_PROPERTY + " or " + KafkaConfig.BOOTSTRAP_SERVER + "] is required !");

        this.config = properties;
    }

    @Override
    public PooledObject<Producer<byte[], byte[]>> makeObject() throws Exception {

        Producer<byte[], byte[]> producer = this.createConnection();

        return new DefaultPooledObject<Producer<byte[], byte[]>>(producer);
    }

    @Override
    public void destroyObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {

        Producer<byte[], byte[]> producer = p.getObject();

        if (null != producer)

            producer.close();
    }

    @Override
    public boolean validateObject(PooledObject<Producer<byte[], byte[]>> p) {

        Producer<byte[], byte[]> producer = p.getObject();

        return (null != producer);
    }

    @Override
    public void activateObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void passivateObject(PooledObject<Producer<byte[], byte[]>> p)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public Producer<byte[], byte[]> createConnection() throws Exception {

        Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(config);

        return producer;
    }
}
