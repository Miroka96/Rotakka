package de.hpi.rotakka.actors.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
}
