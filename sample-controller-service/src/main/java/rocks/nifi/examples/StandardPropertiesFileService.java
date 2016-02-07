package rocks.nifi.examples;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.*;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.BufferedInputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by http://www.nifi.rocks on 2/5/16.
 */

@Tags({"nifirocks", "properties"})
@CapabilityDescription("Provides a controller service to manage property files.")
public class StandardPropertiesFileService extends AbstractControllerService implements PropertiesFileService{
    private static final Logger log = LoggerFactory.getLogger(StandardPropertiesFileService.class);

    public static final PropertyDescriptor CONFIG_URI = new PropertyDescriptor.Builder()
            .name("Configuration Directory")
            .description("Configuration directory for properties files.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RELOAD_INTERVAL = new PropertyDescriptor.Builder()
            .name("Reload Interval")
            .description("Time before looking for changes")
            .defaultValue("60 min")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> serviceProperties;

    static{
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONFIG_URI);
        props.add(RELOAD_INTERVAL);
        serviceProperties = Collections.unmodifiableList(props);
    }

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ScheduledExecutorService executor;
    private Properties properties = new Properties();
    private SynchronousFileWatcher fileWatcher;

    private String configUri;
    private long reloadIntervalMilli;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return serviceProperties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException{
        log.info("Starting properties file service");
        configUri = context.getProperty(CONFIG_URI).getValue();
        reloadIntervalMilli = context.getProperty(RELOAD_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        // Initialize the properties
        loadPropertiesFiles();

        fileWatcher = new SynchronousFileWatcher(Paths.get(configUri), new LastModifiedMonitor());
        executor = Executors.newSingleThreadScheduledExecutor();
        FilesWatcherWorker reloadTask = new FilesWatcherWorker();
        executor.scheduleWithFixedDelay(reloadTask, 0, reloadIntervalMilli, TimeUnit.MILLISECONDS);

    }

    private void loadPropertiesFiles(){
        log.info("Starting loading properties files. (" + configUri + ")");
        File[] propFiles = new File[1];

        lock.readLock().lock();
        try {
            log.info("Read Locked. (" + configUri + ")");
            final File propFile = new File(configUri);

            // Leave room for a single file if that is what is configured

            if (propFile.isDirectory()) {
                log.info("Is directory");
                propFiles = propFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File fileDir, String fileName) {
                        return fileName.endsWith(".properties");
                    }
                });
            } else if (propFile.isFile()) {
                log.info("Is file");
                propFiles[0] = propFile;
            } else {
                log.info("What type of file is it?");
            }
        }finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try{
            log.info("Start loading");
            // Clear all properties
            properties.clear();
            for(File entry : propFiles){
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(entry));
                properties.load(in);
            }
        } catch (FileNotFoundException e) {
            log.error("Could not find file", e);
        } catch (IOException e) {
            log.error("Failed to read file", e);
        } finally{
            lock.writeLock().unlock();
        }

        for(String name : properties.stringPropertyNames()){
            log.info("Set " + name + " => " + properties.get(name));
        }
    }

    private class FilesWatcherWorker implements Runnable {
        @Override
        public void run() {
            lock.readLock().lock();
            try{
                log.info("Check file watcher");
                if(fileWatcher.checkAndReset()){
                    log.error("I found a change?");
                    loadPropertiesFiles();
                }
            } catch (IOException e) {
                log.error("Failed to check file watcher!", e);
            } finally {
                lock.readLock().unlock();
            }
        }
    }
    
    public String getProperty(String key) {
        lock.readLock().lock();
        try{
            return properties.getProperty(key);
        }finally{
            lock.readLock().unlock();
        }
    }


}
