package at.datasciencelabs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;
import com.espertech.esper.client.EventBean;
import com.google.common.collect.Lists;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MapEventTest extends StreamingMultipleProgramsTestBase implements Serializable {

	private static final long serialVersionUID = 8056096050139940300L;

	private static List<ComplexEvent> resultingEvents;

	@Before
	public void setUp() throws Exception {
		resultingEvents = new ArrayList<>();
	}

	@Test
	public void shouldSupportFlattenedMapEvents() throws Exception {
		MapEvent mapStartedEvent = new MapEvent("BuildStartedEvent");
		mapStartedEvent.put("project", "myProject");
		mapStartedEvent.put("buildId", 1);

		MapEvent mapFinishedEvent = new MapEvent("BuildFinishedEvent");
		mapFinishedEvent.put("project", "myProject");
		mapFinishedEvent.put("buildId", 1);

		List<ComplexEvent> expectedValues = Lists.newArrayList();
		ComplexEvent complexEvent = new ComplexEvent(mapStartedEvent, mapFinishedEvent);
		expectedValues.add(complexEvent);

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		executionEnvironment.setParallelism(1);

		List<MapEvent> events = Arrays.asList(mapStartedEvent, mapFinishedEvent);
		DataStream<MapEvent> dataStream = executionEnvironment.fromCollection(events);

		EsperStream<MapEvent> eventEsperStream = Esper.pattern(dataStream, "every(A=BuildStartedEvent(project='myProject')) -> (B=BuildFinishedEvent(project=A.project))");

		DataStream<ComplexEvent> complexEventDataStream = eventEsperStream.select(new EsperSelectFunction<ComplexEvent>() {
			private static final long serialVersionUID = -3360216854308757573L;
			@Override
			public ComplexEvent select(EventBean eventBean) throws Exception {
				return new ComplexEvent((MapEvent)eventBean.get("A"), (MapEvent)eventBean.get("B"));
			}
		});

		complexEventDataStream.addSink(new SinkFunction<ComplexEvent>() {
			private static final long serialVersionUID = -5697228152418028480L;
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
		private MapEvent startEvent;
		private MapEvent endEvent;

		ComplexEvent(MapEvent startEvent, MapEvent endEvent) {
			this.startEvent = startEvent;
			this.endEvent = endEvent;
		}

		MapEvent getStartEvent() {
			return startEvent;
		}

		MapEvent getEndEvent() {
			return endEvent;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ComplexEvent that = (ComplexEvent) o;

			if (!startEvent.equals(that.startEvent)) {
				return false;
			}
			return endEvent.equals(that.endEvent);
		}

		@Override
		public int hashCode() {
			int result = startEvent.hashCode();
			result = 31 * result + endEvent.hashCode();
			return result;
		}
	}


}
