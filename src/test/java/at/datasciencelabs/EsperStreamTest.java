package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

public class EsperStreamTest extends StreamingMultipleProgramsTestBase implements Serializable {

    private static List<TestEvent> result;


    @Before
    public void before() {
        result = new ArrayList<>();
    }

    @Test
    @SuppressWarnings("Convert2Lambda")
    public void shouldSelectFromStreamUsingAnonymousClassSelect() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

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

        resultStream.addSink(new SinkFunction<TestEvent>() {
            @Override
            public void invoke(TestEvent testEvent) throws Exception {
                System.err.println(testEvent);
                result.add(testEvent);
            }
        });

        executionEnvironment.execute("test-2");

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(3));
    }

    @Test
    @SuppressWarnings("Convert2Lambda")
    public void shouldSelectFromStreamUsingLambdaSelect() throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStream<TestEvent> dataStream = executionEnvironment.fromElements(new TestEvent("peter1", 10), new TestEvent("alex1", 25), new TestEvent("maria1", 30));

        EsperStream<TestEvent> esperStream = new EsperStream<>(dataStream, "select name, age from TestEvent");

        DataStream<TestEvent> resultStream = esperStream.select((EsperSelectFunction<TestEvent>) collector -> {
            String name = (String) collector.get("name");
            int age = (int) collector.get("age");
            return new TestEvent(name, age);
        });

        resultStream.addSink(new SinkFunction<TestEvent>() {
            @Override
            public void invoke(TestEvent testEvent) throws Exception {
                result.add(testEvent);
            }
        });

        executionEnvironment.execute("test-1");

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(3));
    }

}