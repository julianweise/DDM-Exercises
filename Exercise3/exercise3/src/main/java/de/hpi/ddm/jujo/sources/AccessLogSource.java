package de.hpi.ddm.jujo.sources;

import de.hpi.ddm.jujo.datatypes.AccessLog;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class AccessLogSource implements SourceFunction<AccessLog> {

    private final String dataFilePath;
    private transient BufferedReader reader;

    public AccessLogSource(String filePath) {
        dataFilePath = filePath;
    }

    @Override
    public void run(SourceContext<AccessLog> sourceContext) throws Exception {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath), StandardCharsets.UTF_8));
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
        }
    }
}
