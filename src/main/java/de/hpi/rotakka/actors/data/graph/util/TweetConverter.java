package de.hpi.rotakka.actors.data.graph.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public final class TweetConverter {
    @NotNull
    public static GraphStoreMaster.Vertex toVertex(@NotNull GraphStoreMaster.TweetData data) {
        GraphStoreMaster.Vertex v = new GraphStoreMaster.Vertex();
        v.setKey("tweet_" + data.tweet_id);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> properties = mapper.convertValue(data, Map.class);
        v.setProperties(properties);
        return v;
    }

    public static GraphStoreMaster.TweetData toTweet(@NotNull GraphStoreMaster.Vertex vertex) {
        ObjectMapper mapper = new ObjectMapper();
        GraphStoreMaster.TweetData tweet = mapper.convertValue(vertex.getProperties(), GraphStoreMaster.TweetData.class);
        return tweet;
    }
}
