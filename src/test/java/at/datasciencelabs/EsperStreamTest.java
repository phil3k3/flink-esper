package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

public class EsperStreamTest implements Serializable {

    @Test
    public void shouldSelectFromStreamUsingLambdaSelect() throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25), new TestEvent("maria", 30));

        EsperStream<TestEvent> esperStream = new EsperStream<>(dataStream, "select name, age from TestEvent");

        DataStream<TestEvent> resultStream = esperStream.select((EsperSelectFunction<TestEvent>) collector -> {
            String name = (String) collector.get("name");
            int age = (int) collector.get("age");
            return new TestEvent(name, age);
        });

        final List<TestEvent> result = new ArrayList<>();
        resultStream.addSink((SinkFunction<TestEvent>) result::add);

        executionEnvironment.execute("test");

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(3));
    }

    @Test
    @SuppressWarnings("Convert2Lambda")
    public void shouldSelectFromStreamUsingAnonymousClassSelect() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25), new TestEvent("maria", 30));

        EsperStream<TestEvent> esperStream = new EsperStream<>(dataStream, "select name, age from TestEvent");

        DataStream<TestEvent> resultStream = esperStream.select(new EsperSelectFunction<TestEvent>() {
            @Override
            public TestEvent select(EventBean eventBean) throws Exception {
                String name = (String) eventBean.get("name");
                int age = (int) eventBean.get("age");
                return new TestEvent(name, age);
            }
        });

        resultStream.printToErr();

        executionEnvironment.execute("test");

        Assert.fail("not finished");
    }

    @Test
    public void shouldCalculateAverageAge() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter", 10), new TestEvent("alex", 25), new TestEvent("maria", 30));

        EsperStream<TestEvent> eventEsperStream = new EsperStream<>(dataStream, "select avg(age) as average_age from TestEvent");

        DataStream<Double> resultStream = eventEsperStream.select((EsperSelectFunction<Double>) collector -> (Double) collector.get("average_age"));

        resultStream.printToErr();

        executionEnvironment.execute("test");

        Assert.fail("not finished");
    }

}