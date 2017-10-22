package at.datasciencelabs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class EsperStreamTest {

    @Test
    public void testEsperStream() throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25), new TestEvent("maria", 30));

        EsperStream<TestEvent> esperStream = new EsperStream<>(dataStream, "select name, age from TestEvent");

        DataStream<TestEvent> resultStream = esperStream.select((EsperSelectFunction<TestEvent>) collector -> {
            String name = (String) collector.get("name");
            int age = (int) collector.get("age");
            return new TestEvent(name, age);
        }, TypeInformation.of(TestEvent.class));

        resultStream.printToErr();

        executionEnvironment.execute("test");

    }

    @Test
    public void testMoreComplexEsperStream() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25), new TestEvent("maria", 30));

        EsperStream<TestEvent> eventEsperStream = new EsperStream<>(dataStream, "select avg(age) as average_age from TestEvent");

        DataStream<Double> resultStream = eventEsperStream.select((EsperSelectFunction<Double>) collector -> {
            Double age = (Double) collector.get("average_age");
            return age;
        }, TypeInformation.of(Double.class));

        resultStream.printToErr();

        executionEnvironment.execute("test");
    }

    @Test
    public void testStateChangeRule() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25));

        EsperStream<TestEvent> eventEsperStream = new EsperStream<>(dataStream, "select avg(age) as average_age from TestEvent");

        DataStream<Double> resultStream = eventEsperStream.select((EsperSelectFunction<Double>) collector -> {
            Double age = (Double) collector.get("average_age");
            return age;
        }, TypeInformation.of(Double.class));

        resultStream.printToErr();

        executionEnvironment.execute("stateChange1");
    }

}