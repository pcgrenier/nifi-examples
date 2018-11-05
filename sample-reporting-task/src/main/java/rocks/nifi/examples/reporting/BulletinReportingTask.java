package rocks.nifi.examples.reporting;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"nifirocks", "bulletin", "metrics"})
@CapabilityDescription("Report bulletin metrics")
public abstract class BulletinReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor SOURCE_ID = new PropertyDescriptor.Builder()
            .name("Source Id")
            .required(false)
            .build();

    public static final PropertyDescriptor MESSAGE_MATCH = new PropertyDescriptor.Builder()
            .name("Message Match")
            .required(false)
            .build();

    public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group Id")
            .required(false)
            .build();


    protected List<PropertyDescriptor> properties;
    private String groupId;
    private String sourceId;
    private String message;

    @Override
    protected void init(final ReportingInitializationContext config){
        properties = new ArrayList<>();
        properties.add(SOURCE_ID);
        properties.add(MESSAGE_MATCH);
        properties.add(GROUP_ID);
    }


    protected AtomicLong lastQuery = new AtomicLong(0);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    protected List<Bulletin> getBulletins(final ReportingContext context) {
        BulletinRepository repo = context.getBulletinRepository();
        BulletinQuery query = getBulletinQuery(context);
        List<Bulletin> bulletins = repo.findBulletins(query);

        if(!bulletins.isEmpty()){
            lastQuery.lazySet(bulletins.get(0).getId());
        }

        return bulletins;
    }

    protected void onScheduled(final ConfigurationContext context) {
        sourceId = context.getProperty(SOURCE_ID).getValue();
        groupId = context.getProperty(GROUP_ID).getValue();
        message = context.getProperty(MESSAGE_MATCH).getValue();
    }

    protected BulletinQuery getBulletinQuery(final ReportingContext context){

        BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder().after(lastQuery.get());

        if(null != sourceId && !sourceId.isEmpty()){
            queryBuilder.sourceIdMatches(sourceId);
        }

        if(null != groupId && !groupId.isEmpty()){
            queryBuilder.groupIdMatches(groupId);
        }

        if(null != message && !message.isEmpty()){
            queryBuilder.messageMatches(message);
        }

        return queryBuilder.build();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final ComponentLog log = getLogger();

        List<Bulletin> bulletins = getBulletins(context);

        bulletins.stream().forEach((bulletin)->log.debug("Hey I found a message=" + bulletin.getMessage()));
    }

}