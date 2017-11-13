package at.datasciencelabs.test;

import at.datasciencelabs.Esper;
import at.datasciencelabs.EsperSelectFunction;
import at.datasciencelabs.EsperStream;
import com.espertech.esper.client.EventBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class FlinkTestClass {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = streamExecutionEnvironment.readTextFile("file:///tmp/flink-test");

        EsperStream<String> esperStream = Esper.pattern(dataStream, "select bytes from String");

        DataStream<String> result = esperStream.select(new EsperSelectFunction<String>() {
            @Override
            public String select(EventBean eventBean) throws Exception {
                return new String((byte[]) eventBean.get("bytes"));
            }
        });

        result.addSink(new PrintSinkFunction<>(true));

        streamExecutionEnvironment.execute("Kafka 0.10 Example");
    }

}
