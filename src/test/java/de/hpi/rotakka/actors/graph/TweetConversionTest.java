package de.hpi.rotakka.actors.graph;

import de.hpi.rotakka.actors.utils.Tweet;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.util.ArrayList;
import java.util.HashMap;

public class TweetConversionTest extends JUnitSuite {

    public Tweet createTweet() {
        Tweet tweet = new Tweet();

        tweet.setTweet_id("tweet_id");
        tweet.setItem_id("item_id");
        tweet.setPermalink("permalink");
        tweet.setConversation_id("conversation_id");
        tweet.setScreen_name("screen_name");
        tweet.setName("name");
        tweet.setUser_id("user_id");
        tweet.setMentions("mentions");
        tweet.setHas_parent_tweet(true);
        tweet.setIs_reply_to(true);
        tweet.setRetweet_id("retweet_id");
        tweet.setRetweeter("retweeter");
        tweet.setType(new ArrayList<>());
        tweet.getType().add("a");
        tweet.getType().add("b");
        tweet.setReferenced_users(new ArrayList<>());
        tweet.getReferenced_users().add("alex");
        tweet.getReferenced_users().add("bob");
        tweet.setTweet_text("tweet_text");
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
    private Tweet tweet = createTweet();

    @Test
    public void tweetToVertex() {
        GraphStoreMaster.Vertex generatedVertex = tweet.toVertex();
        assert (generatedVertex.equals(vertex));
    }

    @Test
    public void vertexToTweet() {
        Tweet generatedTweet = Tweet.createTweet(vertex);
        assert (generatedTweet.equals(tweet));
    }

    @Test
    public void tweetToVertexToTweet() {
        GraphStoreMaster.Vertex generatedVertex = tweet.toVertex();
        Tweet generatedTweet = Tweet.createTweet(generatedVertex);
        assert (generatedTweet.equals(tweet));
    }

    @Test
    public void vertexToTweetToVertex() {
        Tweet generatedTweet = Tweet.createTweet(vertex);
        GraphStoreMaster.Vertex generatedVertex = generatedTweet.toVertex();
        assert (generatedVertex.equals(vertex));
    }

}
