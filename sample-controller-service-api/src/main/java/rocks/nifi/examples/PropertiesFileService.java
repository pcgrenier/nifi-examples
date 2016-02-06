package rocks.nifi.examples;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;

/**
 * Created by phillip on 2/5/16.
 */
@Tags({"nifirocks", "properties"})
@CapabilityDescription("Provides a controller service to manage property files.")
public interface PropertiesFileService extends ControllerService{
    String getProperty(String key);
}
