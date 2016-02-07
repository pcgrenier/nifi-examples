package rocks.nifi.examples;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;

/**
 * Created by http://www.nifi.rocks on 2/5/16.
 */

public interface PropertiesFileService extends ControllerService{
    String getProperty(String key);
}
