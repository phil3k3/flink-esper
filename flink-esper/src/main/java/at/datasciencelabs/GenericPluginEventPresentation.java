package at.datasciencelabs;

import java.util.Iterator;
import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.EventPropertyGetter;
import com.espertech.esper.client.EventPropertyGetterIndexed;
import com.espertech.esper.client.EventPropertyGetterMapped;
import com.espertech.esper.client.EventSender;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.FragmentEventType;
import com.espertech.esper.client.PropertyAccessException;
import com.espertech.esper.core.service.EPRuntimeEventSender;
import com.espertech.esper.plugin.PlugInEventBeanFactory;
import com.espertech.esper.plugin.PlugInEventBeanReflectorContext;
import com.espertech.esper.plugin.PlugInEventRepresentation;
import com.espertech.esper.plugin.PlugInEventRepresentationContext;
import com.espertech.esper.plugin.PlugInEventTypeHandler;
import com.espertech.esper.plugin.PlugInEventTypeHandlerContext;

public class GenericPluginEventPresentation implements PlugInEventRepresentation {
	@Override
	public void init(PlugInEventRepresentationContext eventRepresentationContext) {
		eventRepresentationContext.getEventAdapterService();
	}

	@Override
	public boolean acceptsType(PlugInEventTypeHandlerContext acceptTypeContext) {
		return true;
	}

	@Override
	public PlugInEventTypeHandler getTypeHandler(PlugInEventTypeHandlerContext eventTypeContext) {
		return new PlugInEventTypeHandler() {
			@Override
			public EventType getType() {
				return new EventType() {
					@Override
					public Class getPropertyType(String propertyExpression) {
						// you will have to access the actual fields of the event here and
						// get the type
						return String.class;
					}

					@Override
					public boolean isProperty(String propertyExpression) {
						// you will have to access the actual fields of the event here and
						// check if ist is available
						return propertyExpression.equals("project");
					}

					@Override
					public EventPropertyGetter getGetter(String propertyExpression) {
						return new EventPropertyGetter() {
							@Override
							public Object get(EventBean eventBean) throws PropertyAccessException {
								return null;
							}

							@Override
							public boolean isExistsProperty(EventBean eventBean) {
								return false;
							}

							@Override
							public Object getFragment(EventBean eventBean) throws PropertyAccessException {
								return null;
							}
						};
					}

					@Override
					public FragmentEventType getFragmentType(String propertyExpression) {
						return null;
					}

					@Override
					public Class getUnderlyingType() {
						return null;
					}

					@Override
					public String[] getPropertyNames() {
						return new String[0];
					}

					@Override
					public EventPropertyDescriptor[] getPropertyDescriptors() {
						return new EventPropertyDescriptor[0];
					}

					@Override
					public EventPropertyDescriptor getPropertyDescriptor(String propertyName) {
						return null;
					}

					@Override
					public EventType[] getSuperTypes() {
						return new EventType[0];
					}

					@Override
					public Iterator<EventType> getDeepSuperTypes() {
						return null;
					}

					@Override
					public String getName() {
						return eventTypeContext.getEventTypeName();
					}

					@Override
					public EventPropertyGetterMapped getGetterMapped(String mappedPropertyName) {
						return null;
					}

					@Override
					public EventPropertyGetterIndexed getGetterIndexed(String indexedPropertyName) {
						return null;
					}

					@Override
					public int getEventTypeId() {
						return 0;
					}

					@Override
					public String getStartTimestampPropertyName() {
						return null;
					}

					@Override
					public String getEndTimestampPropertyName() {
						return null;
					}
				};
			}

			@Override
			public EventSender getSender(EPRuntimeEventSender runtimeEventSender) {
				return null;
			}
		};
	}

	@Override
	public boolean acceptsEventBeanResolution(PlugInEventBeanReflectorContext acceptBeanContext) {
		return false;
	}

	@Override
	public PlugInEventBeanFactory getEventBeanFactory(PlugInEventBeanReflectorContext eventBeanContext) {
		return null;
	}
}
