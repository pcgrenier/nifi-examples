package rocks.nifi.examples.reporting;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"nifirocks", "influxdb", "bulletin", "metrics"})
@CapabilityDescription("Report bulletin metrics to the InfluxDB time series database")
public class InfluxBulletinReportingTask extends BulletinReportingTask {

    public static final Validator INFLUX_HOST_VALIDATOR = (String subject, String input, ValidationContext context) -> {
        ValidationResult.Builder builder = new ValidationResult.Builder()
                .subject(subject)
                .input(input);

        try {
            InfluxDB influxDB = InfluxDBFactory.connect(input, "user", "pass");
            Pong p = influxDB.ping();
            if (p.getResponseTime() > 0) {
                builder.valid(true).explanation("connected to " + input + " in " + p.getResponseTime());
            } else {
                builder.valid(false).explanation("Failed to connect to " + input);
            }
        } catch (final IllegalArgumentException e) {
            builder.valid(false).explanation(e.getMessage());
        }

        return builder.build();
    };

    public static final PropertyDescriptor INFLUXDB_PATH = new PropertyDescriptor.Builder()
            .name("Influxdb path")
            .required(true)
            .addValidator(INFLUX_HOST_VALIDATOR)
            .defaultValue("http://localhost:8086")
            .build();

    public static final PropertyDescriptor INFLUXDB_NAME = new PropertyDescriptor.Builder()
            .name("Influxdb database name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUXDB_METRIC = new PropertyDescriptor.Builder()
            .name("Influxdb metric name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public void init(final ReportingInitializationContext context) {
        super.init(context);

        properties.add(INFLUXDB_PATH);
        properties.add(INFLUXDB_NAME);
        properties.add(INFLUXDB_METRIC);

        properties = Collections.unmodifiableList(properties);
    }

    private String host;
    private String name;
    private String metric;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("tag name and value")
                .dynamic(true)
                .expressionLanguageSupported(true)
                .required(false)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        // setup, to initiate stuff based off of context that will not change between onTriggers
        super.onScheduled(context);
        host = context.getProperty(INFLUXDB_PATH).getValue();
        name = context.getProperty(INFLUXDB_NAME).getValue();
        metric = context.getProperty(INFLUXDB_METRIC).getValue();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final long timestamp = System.currentTimeMillis();

        Map<String, String> tags = new HashMap<>();
        context.getProperties().entrySet().stream()
                .filter((entry) -> entry.getKey().isDynamic())
                .forEach(entry -> tags.put(entry.getKey().getName(), entry.getValue()));

        InfluxDB influxDB = InfluxDBFactory.connect(host, "user", "pass");
        BatchPoints.Builder batch = BatchPoints.database(name);

        tags.forEach((key, value) -> batch.tag(key, value));

        try {
            Map<String, AtomicInteger> counts = new HashMap<>();
            counts.put("INFO", new AtomicInteger(0));
            counts.put("WARNING", new AtomicInteger(0));
            counts.put("ERROR", new AtomicInteger(0));

            List<Bulletin> bulletins = getBulletins(context);
            for (Bulletin bulletin : bulletins) {
                if (bulletin.getLevel().equalsIgnoreCase("INFO")) {
                    counts.get("INFO").incrementAndGet();
                } else if (bulletin.getLevel().equalsIgnoreCase("WARNING")) {
                    counts.get("WARNING").incrementAndGet();
                } else if (bulletin.getLevel().equalsIgnoreCase("ERROR")) {
                    counts.get("ERROR").incrementAndGet();
                }
            }

            counts.forEach((key, value) -> {
                Point.Builder point = Point.measurement(metric)
                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .tag(metric, key)
                        .addField("value", value.intValue());
                batch.point(point.build());
            });

            influxDB.write(batch.build());

        } finally {
            influxDB.close();
        }
    }

}
