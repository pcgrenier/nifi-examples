/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rocks.nifi.examples.processors;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import rocks.nifi.examples.PropertiesFileService;
import rocks.nifi.examples.StandardPropertiesFileService;

import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author phillip
 */
public class ControllerServiceProcessorTest {

    private TestRunner runner;
    private PropertiesFileService propertiesService;
    private Map<String, String> propertiesServiceProperties;

    @Before
    public void setup() throws InitializationException {
        propertiesService = new StandardPropertiesFileService();

        propertiesServiceProperties = new HashMap<>();
        propertiesServiceProperties.put(StandardPropertiesFileService.RELOAD_INTERVAL.getName(), "30 sec");

        // Generate a test runner to mock a processor in a flow
        runner = TestRunners.newTestRunner(new ControllerServiceProcessor());
    }

    /**
     * Test of onTrigger method, of class ControllerServiceProcessor.
     */
    @org.junit.Test
    public void testOnTrigger() throws IOException, InitializationException {
        String propFile = this.getClass().getClassLoader().getResource("test.properties").getFile();
        propertiesServiceProperties.put(StandardPropertiesFileService.CONFIG_URI.getName(), propFile);

        // Add controller service
        runner.addControllerService("propertiesServiceTest", propertiesService, propertiesServiceProperties);
        runner.enableControllerService(propertiesService);
        runner.setProperty(ControllerServiceProcessor.PROPERTIES_SERVICE, "propertiesServiceTest");

        // Content to be mock a json file
        InputStream content = this.getClass().getClassLoader().getResourceAsStream("test.json");

        // Add properties
        runner.setProperty(ControllerServiceProcessor.PROPERTY_NAME, "hello");

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ControllerServiceProcessor.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        // Test attributes and content
        result.assertAttributeEquals("property", "nifi.rocks.prop");
    }

    /**
     * Test of onTrigger method, of class ControllerServiceProcessor using a directory.
     */
    @org.junit.Test
    public void testPropertiesDirectory() throws IOException, InitializationException {
        String propFile = this.getClass().getClassLoader().getResource("properties").getPath();
        propertiesServiceProperties.put(StandardPropertiesFileService.CONFIG_URI.getName(), propFile);

        // Add controller service
        runner.addControllerService("propertiesServiceTest", propertiesService, propertiesServiceProperties);
        runner.enableControllerService(propertiesService);
        runner.setProperty(ControllerServiceProcessor.PROPERTIES_SERVICE, "propertiesServiceTest");

        testProperty("hello", "nifi.rocks.prop");
        testProperty("hello.again", "nifi.rocks.prop.2");
    }

    private void testProperty(String name, String resultValue){
        // Clear previous results
        runner.clearTransferState();

        // Add properties
        runner.setProperty(ControllerServiceProcessor.PROPERTY_NAME, name);

        // Dummy bytes
        runner.enqueue("TEST".getBytes());

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ControllerServiceProcessor.SUCCESS);
        assertTrue("1 match actual => " + results.size(), results.size() == 1);
        MockFlowFile result = results.get(0);

        // Test attributes and content
        result.assertAttributeEquals("property", resultValue);
    }

}
