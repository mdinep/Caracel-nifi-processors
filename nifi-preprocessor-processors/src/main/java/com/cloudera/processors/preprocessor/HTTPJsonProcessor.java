package com.cloudera.processors.preprocessor;

import com.google.gson.*;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"HTTP Json Processor","JSON","HTTP","DPI","HTTP Log"})
@CapabilityDescription("Preprocess JSON data for HTTP requests and responses for indexing with Metron")
@SeeAlso({})
@ReadsAttributes({
        @ReadsAttribute(attribute="dst_ip", description="Destination IP"),
        @ReadsAttribute(attribute="dst_port", description="Destination Port"),
        @ReadsAttribute(attribute="expiration_ts", description="Expiration Timestamp"),
        @ReadsAttribute(attribute="flow_id", description="Flow ID"),
        @ReadsAttribute(attribute="path", description="Path"),
        @ReadsAttribute(attribute="probe-id", description="Data Collection Probe ID"),
        @ReadsAttribute(attribute="src_ip", description="Source IP"),
        @ReadsAttribute(attribute="src_port", description="Source Port"),
        @ReadsAttribute(attribute="start_ts", description="Starting Timestamp"),
        @ReadsAttribute(attribute="timestamp", description="Record Timestamp"),
        @ReadsAttribute(attribute="version", description="Version Number")
})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class HTTPJsonProcessor extends AbstractProcessor{

    public static final PropertyDescriptor DELIMITER_PROPERTY = new PropertyDescriptor
            .Builder().name("DELIMITER_PROPERTY")
            .displayName("Field Delimiter")
            .description("Delimiter character to use in JSON keys")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("_")
            .build();

    public static final PropertyDescriptor JSON_TAGS_PROPERTY = new PropertyDescriptor
            .Builder().name("JSON_TAGS")
            .displayName("JSON Tags field value")
            .description("Value to set for JSON tags top level field in JSON output")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("[]")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("Original")
            .description("The original HTTP Json input")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successfully parsed HTTP JSON data")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed to parse HTTP JSON Data")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DELIMITER_PROPERTY);
        descriptors.add(JSON_TAGS_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private volatile String keyDelimiter;
    private volatile String jsonTags;

    @OnScheduled
    public void onSchedule(ProcessContext context) {
        this.keyDelimiter = context.getProperty(DELIMITER_PROPERTY).getValue();
        this.jsonTags = context.getProperty(JSON_TAGS_PROPERTY).getValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        this.keyDelimiter = context.getProperty(DELIMITER_PROPERTY).getValue();
        this.jsonTags = context.getProperty(JSON_TAGS_PROPERTY).getValue();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final AtomicReference<String> value = new AtomicReference<>();
        int indexVal = 0;

        JsonArray newArray = new JsonArray();
        Map<String, String> defaultMap = buildDefaultValues(flowFile);
        ArrayList<FlowFile> splitFlowFiles = new ArrayList<>();

        session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) {
                        try {
                            String json = IOUtils.toString(in, "UTF-8");
                            value.set(json);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            getLogger().error("Failed to read json string.");
                        }
                    }
                });


        String content = value.get();
        if(content != null && !content.isEmpty()){
            JsonParser parser = new JsonParser();
            try{
                JsonObject jsonObj = parser.parse(content).getAsJsonObject();
                JSONArray reqArray = JsonPath.read(jsonObj.toString(),"$['http-stream']['http.request']");
                JsonArray requestArray = parser.parse(reqArray.toString()).getAsJsonArray();

                for (JsonElement eo : requestArray) {
                    JsonObject expObject = new JsonObject();
                    JsonObject iterObject = eo.getAsJsonObject();
                    for (Map.Entry<String, JsonElement> entry : iterObject.entrySet()) {
                        String newKey = entry.getKey().replaceAll("\\.", this.keyDelimiter);
                        JsonElement newVal = entry.getValue();
                        expObject.add(newKey, newVal);
                    }

                    Gson gson = new Gson();

                    JsonObject baseObject = createJsonObject(parser, gson, defaultMap);

                    String indexJsonVal = "http" + this.keyDelimiter + "index";
                    Integer indexValFromData = expObject.get(indexJsonVal).getAsInt();
                    if (indexValFromData != indexVal) {
                        indexVal = indexValFromData;
                        JsonObject updatedExpObject =cleanKeys(expObject);
                        newArray.add(updatedExpObject);

                    } else {
                        JsonObject updatedExpObject = cleanKeys(expObject);
                        newArray.add(updatedExpObject);
                        JsonObject newObject = baseObject;
                        newObject.add("http", newArray);
                        FlowFile newFF = session.create(flowFile);
                        newFF = session.write(newFF, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write(newObject.toString().getBytes());
                            }
                        });
                        session.putAttribute(newFF, "mime.type","application/json");
                        splitFlowFiles.add(newFF);
                        newArray = new JsonArray();
                    }
                }
                session.remove(flowFile);
                session.transfer(splitFlowFiles, REL_SUCCESS);

            } catch (Exception ex) {
                ex.printStackTrace();
                getLogger().error("Unable to parse FlowFile content as JSON: " + ex.toString());
                session.transfer(flowFile, REL_FAILURE);
            }

        } else {
            getLogger().error("Failed to retrieve FlowFile content.");
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    public Map<String, String> buildDefaultValues(FlowFile flowFile){
        Map<String, String> defaultMap = new HashMap<>();
        defaultMap.put("dst_ip", flowFile.getAttribute("dst_ip"));
        defaultMap.put("dst_port", flowFile.getAttribute("dst_port"));
        defaultMap.put("expiration_ts", flowFile.getAttribute("expiration_ts"));
        defaultMap.put("flow_id", flowFile.getAttribute("flow_id"));
        defaultMap.put("path", flowFile.getAttribute("path"));
        defaultMap.put("probe-id", flowFile.getAttribute("probe-id"));
        defaultMap.put("src_ip", flowFile.getAttribute("src_ip"));
        defaultMap.put("src_port", flowFile.getAttribute("src_port"));
        defaultMap.put("start_ts", flowFile.getAttribute("start_ts"));
        defaultMap.put("timestamp", flowFile.getAttribute("timestamp"));
        defaultMap.put("version", flowFile.getAttribute("version"));
        defaultMap.put("tags", this.jsonTags);

        return defaultMap;
    }

    public JsonObject createJsonObject(JsonParser parser, Gson gson, Map<String, String> defaultMap){
        JsonObject baseObject = parser.parse(gson.toJson(defaultMap)).getAsJsonObject();
        return baseObject;
    }

    public JsonObject cleanKeys(JsonObject inputObj){
        JsonObject outObj = new JsonObject();
        for (Map.Entry<String, JsonElement> entry : inputObj.entrySet()) {
            String newKey = entry.getKey();
            newKey = newKey.substring(newKey.indexOf("_") + 1);
            outObj.add(newKey, entry.getValue());
        }
        return outObj;
    }
}
