package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class EsperPatternTest implements Serializable {


    private static List<ComplexEvent> resultingEvents;

    @Before
    public void setUp() throws Exception {
        resultingEvents = Lists.newArrayList();
    }

    @Test
    public void testEsperPattern() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        List<ComplexEvent> expectedValues = Lists.newArrayList();
        ComplexEvent complexEvent = new ComplexEvent(Event.start(), Event.end());
        expectedValues.add(complexEvent);

        List<Event> events = Arrays.asList(complexEvent.getStartEvent(), complexEvent.getEndEvent());
        DataStream<Event> dataStream = executionEnvironment.fromCollection(events);

        EsperStream<Event> esperStream = Esper.pattern(dataStream, "every (A=Event(type='start') -> B=Event(type='end'))");

        DataStream<ComplexEvent> complexEventDataStream = esperStream.select(new EsperSelectFunction<ComplexEvent>() {
            @Override
            public ComplexEvent select(EventBean eventBean) throws Exception {
                return new ComplexEvent((Event) eventBean.get("A"), (Event) eventBean.get("B"));
            }
        });

        complexEventDataStream.addSink(new SinkFunction<ComplexEvent>() {
            @Override
            public void invoke(ComplexEvent value) throws Exception {
                System.err.println(value);
                resultingEvents.add(value);
            }
        });

        executionEnvironment.execute("test-2");

        assertThat(resultingEvents, is(expectedValues));
    }

    private static class ComplexEvent {
        private Event startEvent;
        private Event endEvent;

        ComplexEvent(Event startEvent, Event endEvent) {
            this.startEvent = startEvent;
            this.endEvent = endEvent;
        }

        Event getStartEvent() {
            return startEvent;
        }

        Event getEndEvent() {
            return endEvent;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ComplexEvent that = (ComplexEvent) o;

            if (startEvent != null ? !startEvent.equals(that.startEvent) : that.startEvent != null) return false;
            return endEvent != null ? endEvent.equals(that.endEvent) : that.endEvent == null;
        }

        @Override
        public int hashCode() {
            int result = startEvent != null ? startEvent.hashCode() : 0;
            result = 31 * result + (endEvent != null ? endEvent.hashCode() : 0);
            return result;
        }
    }

    public static class Event {

        private String type;

        private Event(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        static Event start() {
            return new Event("start");
        }

        static Event end() {
            return new Event("end");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Event event = (Event) o;

            return type != null ? type.equals(event.type) : event.type == null;
        }

        @Override
        public int hashCode() {
            return type != null ? type.hashCode() : 0;
        }
    }
}
