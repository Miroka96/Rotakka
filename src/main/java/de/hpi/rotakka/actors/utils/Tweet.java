package de.hpi.rotakka.actors.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Edge;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.ExtendableSubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.SubGraph;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Vertex;

@NoArgsConstructor
@Data
public class Tweet {
    private String tweet_id;
    private String item_id;
    private String permalink;
    private String conversation_id;
    private String screen_name;
    private String name;
    private String user_id;
    private String mentions;

    private Boolean has_parent_tweet;
    private Boolean is_reply_to;

    private String retweet_id;
    private String retweeter;

    private List<String> type = new ArrayList<>();
    private List<String> referenced_users = new ArrayList<>();

    private String tweet_text;

    public Tweet(@NotNull Element tweetElement) {
        tweet_id = tweetElement.attributes().get("data-tweet-id");
        item_id = tweetElement.attributes().get("data-item-id");
        permalink = tweetElement.attributes().get("data-permalink-path");
        conversation_id = tweetElement.attributes().get("data-conversation-id");
        screen_name = tweetElement.attributes().get("data-screen-name");
        name = tweetElement.attributes().get("data-name");
        user_id = tweetElement.attributes().get("data-user-id");

        if(tweetElement.attributes().hasKey("data-mentions")) {
            mentions = tweetElement.attributes().get("data-mentions");
            referenced_users.addAll(Arrays.asList(mentions.split(" ")));
        }

        if(tweetElement.attributes().hasKey("data-has-parent-tweet")) {
            has_parent_tweet = Boolean.parseBoolean(tweetElement.attributes().get("data-has-parent-tweet"));
        }

        if (tweetElement.attributes().hasKey("data-retweet-id")) {
            retweet_id = tweetElement.attributes().get("data-retweet-id");
            retweeter = tweetElement.attributes().get("data-retweeter");
            referenced_users.add(screen_name);
            type.add("retweet");
        }
        if (tweetElement.attributes().hasKey("data-is-reply-to")) {
            is_reply_to = Boolean.parseBoolean(tweetElement.attributes().get("data-is-reply-to"));
            type.add("reply");
        }
        if(type.size() == 0) {
            type.add("tweet");
        }

        tweet_text = tweetElement.children().select("div[class=content]").get(0).children().select("div[class=js-tweet-text-container]").text();
    }

    public static Tweet createTweet(@NotNull Vertex vertex) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(vertex.getProperties(), Tweet.class);
    }

    public Vertex toVertex() {
        Vertex v = new Vertex();
        v.setKey("tweet_" + this.tweet_id);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> properties = mapper.convertValue(this, Map.class);
        v.setProperties(properties);
        return v;
    }

    public SubGraph toSubGraph() {
        ExtendableSubGraph subGraph = new ExtendableSubGraph();

        // tweet vertex
        Vertex tweetVertex = this.toVertex();
        subGraph.vertices.add(tweetVertex);

        // creator vertex
        User creator = new User();
        creator.user_id = user_id;
        creator.name = name;
        creator.screen_name = screen_name;
        Vertex creatorVertex = creator.toVertex();
        subGraph.vertices.add(creatorVertex);

        // created edge
        Edge createdEdge = new Edge();
        createdEdge.from = creatorVertex.key;
        createdEdge.to = tweetVertex.key;
        createdEdge.key = "created_" + screen_name + "_" + tweet_id;
        subGraph.edges.add(createdEdge);

        // referenced users vertices
        List<Vertex> referencedUsersVertices = this.referenced_users.stream().map(screen_name -> {
            User referencedUser = new User();
            referencedUser.screen_name = screen_name;
            return referencedUser.toVertex();
        }).collect(Collectors.toList());
        subGraph.vertices.addAll(referencedUsersVertices);

        // references edges
        List<Edge> referencesEdges = referencedUsersVertices.stream().map(referencedUser -> {
            Edge references = new Edge();
            references.from = tweetVertex.key;
            references.to = referencedUser.key;
            references.key = "references_" + tweet_id + "_" +
                    referencedUser.properties.getOrDefault("screen_name", "null");
            return references;
        }).collect(Collectors.toList());
        subGraph.edges.addAll(referencesEdges);

        return subGraph.toSubGraph();
    }
}
