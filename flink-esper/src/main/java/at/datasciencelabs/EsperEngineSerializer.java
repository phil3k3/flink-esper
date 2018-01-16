package at.datasciencelabs;

import com.espertech.esper.client.EPServiceProvider;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class EsperEngineSerializer extends TypeSerializer<EPServiceProvider> {

    private static final long serialVersionUID = -5523322497977330283L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<EPServiceProvider> duplicate() {
        return this;
    }

    @Override
    public EPServiceProvider createInstance() {
        return null;
    }

    @Override
    public EPServiceProvider copy(EPServiceProvider from) {
        return null;
    }

    @Override
    public EPServiceProvider copy(EPServiceProvider from, EPServiceProvider reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(EPServiceProvider record, DataOutputView target) throws IOException {

    }

    @Override
    public EPServiceProvider deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public EPServiceProvider deserialize(EPServiceProvider reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerConfigSnapshot snapshotConfiguration() {
        return null;
    }

    @Override
    public CompatibilityResult<EPServiceProvider> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
        return null;
    }
}
