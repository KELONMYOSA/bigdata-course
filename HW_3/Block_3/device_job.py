from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction

SOURCE_TOPIC_NAME = "block3"
SOURCE_GROUP_ID = "pyflink-e2e-source-block3"
SOURCE_NAME = "Kafka Source - Block 3"
SINK_TOPIC_NAME = "block3-processed"


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics(SOURCE_TOPIC_NAME) \
        .set_group_id(SOURCE_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic(SINK_TOPIC_NAME)
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), SOURCE_NAME)
    ds.map(TemperatureFunction(), Types.STRING()) \
        .sink_to(sink)
    env.execute_async("Devices preprocessing")


class TemperatureFunction(MapFunction):
    def map(self, value):
        device_id, temperature, execution_time = value
        return str({"device_id": device_id, "temperature": temperature - 273, "execution_time": execution_time})


if __name__ == '__main__':
    python_data_stream_example()
