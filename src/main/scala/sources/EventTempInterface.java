package sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface EventTempInterface<T> extends SourceFunction<T> {
}