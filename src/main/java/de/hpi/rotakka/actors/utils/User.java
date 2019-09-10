package de.hpi.rotakka.actors.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import lombok.Data;

import java.util.Map;

@Data
public class User {
    public String user_id; // e.g. "87818409"
    public String name; // e.g. "The Guardian"
    public String screen_name; // e.g. "guardian"

    public GraphStoreMaster.Vertex toVertex() {
        GraphStoreMaster.Vertex v = new GraphStoreMaster.Vertex();
        v.setKey("user_" + this.screen_name);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> properties = mapper.convertValue(this, Map.class);
        v.setProperties(properties);
        return v;
    }
}
