package de.hpi.ddm.jujo.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AkkaUtils {

    private static class VariableBinding {

        private final String pattern, value;

        private VariableBinding(String variableName, Object value) {
            this.pattern = Pattern.quote("$" + variableName);
            this.value = Objects.toString(value);
        }

        private String apply(String str) {
            return str.replaceAll(this.pattern, this.value);
        }
    }

    private static Config loadConfig(String resource, VariableBinding... bindings) {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new FileNotFoundException("Could not get the resource " + resource);
            }
            Stream<String> content = new BufferedReader(new InputStreamReader(in)).lines();
            for (VariableBinding binding : bindings) {
                content = content.map(binding::apply);
            }
            String result = content.collect(Collectors.joining("\n"));
            return ConfigFactory.parseString(result);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load resource " + resource);
        }
    }


    public static Config createRemoteAkkaConfig(String host, int port) {
        Config baseConfig = loadConfig("base.conf");
        Config remoteConfig = loadConfig(
                "remote.conf",
                new VariableBinding("host", host),
                new VariableBinding("port", port)
        );
        return remoteConfig.withFallback(baseConfig);
    }

    public static String SHA256(int data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashedBytes = digest.digest(String.valueOf(data).getBytes(StandardCharsets.UTF_8));

        StringBuilder stringBuffer = new StringBuilder();
        for (byte hashedByte : hashedBytes) {
            stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
        }
        return stringBuffer.toString();
    }
}
