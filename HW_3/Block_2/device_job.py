from pyflink.common import SimpleStringSchema, Time
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction, ReduceFunction, KeySelector
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows, \
    ProcessingTimeSessionWindows

SOURCE_TOPIC_NAME = "block2"
SOURCE_GROUP_ID = "pyflink-e2e-source-block2"
SOURCE_NAME = "Kafka Source - Block 2"
SINK_TOPIC_NAME = "block2-processed"
# "tumbling", "sliding" or "session" to turn off - None
WINDOW_TYPE = "session"


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

    if WINDOW_TYPE is "tumbling":
        ds.window_all(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
            .reduce(TemperatureFunctionReducer(), output_type=type_info) \
            .map(TemperatureFunctionMaper(), Types.STRING()) \
            .sink_to(sink)
    elif WINDOW_TYPE is "sliding":
        ds.window_all(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
            .reduce(TemperatureFunctionReducer(), output_type=type_info) \
            .map(TemperatureFunctionMaper(), Types.STRING()) \
            .sink_to(sink)
    elif WINDOW_TYPE is "session":
        ds.key_by(DeviceKeySelector(), Types.LONG()) \
            .window(ProcessingTimeSessionWindows.with_gap(Time.seconds(5))) \
            .reduce(TemperatureFunctionReducer(), output_type=type_info) \
            .map(TemperatureFunctionMaper(), Types.STRING()) \
            .sink_to(sink)
    else:
        ds.map(TemperatureFunctionMaper(), Types.STRING()).sink_to(sink)

    env.execute_async("Devices preprocessing")


class DeviceKeySelector(KeySelector):
    def get_key(self, value):
        device_id, _, _ = value
        return device_id


class TemperatureFunctionMaper(MapFunction):
    def map(self, value):
        device_id, temperature, execution_time = value
        return str({"device_id": device_id, "temperature": temperature - 273, "execution_time": execution_time})


class TemperatureFunctionReducer(ReduceFunction):
    def reduce(self, value1, value2):
        temperature1 = value1[1]
        temperature2 = value2[1]

        if temperature1 > temperature2:
            return value1
        else:
            return value2


if __name__ == '__main__':
    python_data_stream_example()
