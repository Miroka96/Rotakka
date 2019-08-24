package de.hpi.rotakka.actors.data.graph;

import de.hpi.rotakka.actors.data.graph.util.TweetConverter;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.util.ArrayList;
import java.util.HashMap;

public class TweetConverterTest extends JUnitSuite {

    public GraphStoreMaster.TweetData createTweet() {
        GraphStoreMaster.TweetData tweet = new GraphStoreMaster.TweetData();
        tweet.tweet_id = "tweet_id";
        tweet.item_id = "item_id";
        tweet.permalink = "permalink";
        tweet.conversation_id = "conversation_id";
        tweet.screen_name = "screen_name";
        tweet.name = "name";
        tweet.user_id = "user_id";
        tweet.mentions = "mentions";
        tweet.has_parent_tweet = true;
        tweet.is_reply_to = true;
        tweet.retweet_id = "retweet_id";
        tweet.retweeter = "retweeter";
        tweet.type = new ArrayList<>();
        tweet.type.add("a");
        tweet.type.add("b");
        tweet.referenced_users = new ArrayList<>();
        tweet.referenced_users.add("alex");
        tweet.referenced_users.add("bob");
        tweet.tweet_text = "tweet_text";
        return tweet;
    }

    public GraphStoreMaster.Vertex createVertex() {
        GraphStoreMaster.Vertex vertex = new GraphStoreMaster.Vertex();
        vertex.setKey("tweet_tweet_id");
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("tweet_id", "tweet_id");
        properties.put("item_id", "item_id");
        properties.put("permalink", "permalink");
        properties.put("conversation_id", "conversation_id");
        properties.put("screen_name", "screen_name");
        properties.put("name", "name");
        properties.put("user_id", "user_id");
        properties.put("mentions", "mentions");
        properties.put("has_parent_tweet", true);
        properties.put("is_reply_to", true);
        properties.put("retweet_id", "retweet_id");
        properties.put("retweeter", "retweeter");
        ArrayList<String> types = new ArrayList<>();
        types.add("a");
        types.add("b");
        properties.put("type", types);

        ArrayList<String> users = new ArrayList<>();
        users.add("alex");
        users.add("bob");
        properties.put("referenced_users", users);
        properties.put("tweet_text", "tweet_text");
        vertex.setProperties(properties);

        return vertex;
    }

    private GraphStoreMaster.Vertex vertex = createVertex();
    private GraphStoreMaster.TweetData tweet = createTweet();

    @Test
    public void tweetToVertex() {
        GraphStoreMaster.Vertex generatedVertex = TweetConverter.toVertex(tweet);
        assert (generatedVertex.equals(vertex));
    }

    @Test
    public void vertexToTweet() {
        GraphStoreMaster.TweetData generatedTweet = TweetConverter.toTweet(vertex);
        assert (generatedTweet.equals(tweet));
    }

    @Test
    public void tweetToVertexToTweet() {
        GraphStoreMaster.Vertex generatedVertex = TweetConverter.toVertex(tweet);
        GraphStoreMaster.TweetData generatedTweet = TweetConverter.toTweet(generatedVertex);
        assert (generatedTweet.equals(tweet));
    }

    @Test
    public void vertexToTweetToVertex() {
        GraphStoreMaster.TweetData generatedTweet = TweetConverter.toTweet(vertex);
        GraphStoreMaster.Vertex generatedVertex = TweetConverter.toVertex(generatedTweet);
        assert (generatedVertex.equals(vertex));
    }

}
