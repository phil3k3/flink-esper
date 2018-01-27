package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.avro.SchemaBuilder.record;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AvroEventTest extends StreamingMultipleProgramsTestBase implements Serializable {

	private static final long serialVersionUID = 8056096050139940300L;

	private static List<ComplexEvent> resultingEvents;
	private static Schema started;
	private static Schema finished;
	private static HashMap<String,Schema> types;

	@Before
	public void setUp() throws Exception {
		resultingEvents = new ArrayList<>();
		started = record("BuildStartedEvent").fields()
				.requiredString("project")
				.requiredInt("buildId")
				.endRecord();
		finished = record("BuildFinishedEvent").fields()
				.requiredString("project")
				.requiredInt("buildId")
				.endRecord();
		types = new HashMap<>();

		types.put("BuildStartedEvent", started);
		types.put("BuildFinishedEvent", finished);
	}

	@Test
	public void shouldSupportFlattenedMapEvents() throws Exception {

		GenericData.Record mapStartedEvent = new GenericData.Record(started);
		mapStartedEvent.put("project", "myProject");
		mapStartedEvent.put("buildId", 1);

		GenericData.Record mapFinishedEvent = new GenericData.Record(finished);
		mapFinishedEvent.put("project", "myProject");
		mapFinishedEvent.put("buildId", 1);

		List<ComplexEvent> expectedValues = Lists.newArrayList();
		ComplexEvent complexEvent = new ComplexEvent(mapStartedEvent, mapFinishedEvent);
		expectedValues.add(complexEvent);

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		executionEnvironment.setParallelism(1);

		List<GenericData.Record> events = Arrays.asList(mapStartedEvent, mapFinishedEvent);
		DataStream<GenericData.Record> dataStream = executionEnvironment.fromCollection(events);

		SchemaProvider schemaProvider = (SchemaProvider) () -> types;
		EsperStream<GenericData.Record> eventEsperStream = Esper
				.pattern(dataStream, "every(A=BuildStartedEvent(project='myProject')) -> (B=BuildFinishedEvent(project=A.project))", schemaProvider);

		DataStream<ComplexEvent> complexEventDataStream = eventEsperStream.select(new EsperSelectFunction<ComplexEvent>() {
			private static final long serialVersionUID = -3360216854308757573L;

			@Override
			public ComplexEvent select(EventBean eventBean) throws Exception {
				return new ComplexEvent((GenericData.Record) eventBean.get("A"), (GenericData.Record) eventBean.get("B"));
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
		private GenericData.Record startEvent;
		private GenericData.Record endEvent;

		ComplexEvent(GenericData.Record startEvent, GenericData.Record endEvent) {
			this.startEvent = startEvent;
			this.endEvent = endEvent;
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

			return startEvent.equals(that.startEvent) && endEvent.equals(that.endEvent);
		}

		@Override
		public int hashCode() {
			int result = startEvent.hashCode();
			result = 31 * result + endEvent.hashCode();
			return result;
		}
	}
}