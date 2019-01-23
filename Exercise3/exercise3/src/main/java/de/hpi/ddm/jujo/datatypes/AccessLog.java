package de.hpi.ddm.jujo.datatypes;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccessLog implements Serializable {

    public boolean isValid;
    public String clientAddress;
    public String clientIdentity;
    public String clientAuthenticationId;
    public LocalDateTime timestamp;
    public int timeZone;
    public String httpMethod;
    public String pathToResource;
    public String httpProtocol;
    public int statusCode;
    public int resourceSize;
    public String rawLine;

    private static DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

    public static AccessLog fromString(String line) {
        String[] tokens = line.split(" ");
        AccessLogBuilder builder = new AccessLogBuilder();

        if (tokens.length != 10) {
            return builder
                    .isValid(false)
                    .rawLine(line)
                    .build();
        }

        return builder
                .isValid(true)
                .clientAddress(tokens[0])
                .clientIdentity(tokens[1])
                .clientAuthenticationId(tokens[2])
                .timestamp(LocalDateTime.parse(tokens[3].replace("[", ""), timeFormatter))
                .timeZone(Integer.parseInt(tokens[4].replace("+", "").replace("]", "")))
                .httpMethod(tokens[5].replace("\"", ""))
                .pathToResource(tokens[6])
                .httpProtocol(tokens[7].replace("\"", ""))
                .statusCode(Integer.parseInt(tokens[8]))
                .resourceSize(Integer.parseInt(tokens[9].replace("-", "0")))
                .rawLine(line)
                .build();
    }
}
