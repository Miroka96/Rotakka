package de.hpi.rotakka.actors.utils;

import lombok.Getter;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;

@Getter
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

    private List<String> type;

    private String tweet_text;

    public Tweet(Element tweetElement) {
        type = new ArrayList<>();

        tweet_id = tweetElement.attributes().get("data-tweet-id");
        item_id = tweetElement.attributes().get("data-item-id");
        permalink = tweetElement.attributes().get("data-permalink-path");
        conversation_id = tweetElement.attributes().get("data-conversation-id");
        screen_name = tweetElement.attributes().get("data-screen-name");
        name = tweetElement.attributes().get("data-name");
        user_id = tweetElement.attributes().get("data-user-id");

        if(tweetElement.attributes().hasKey("mentions")) {
            mentions = tweetElement.attributes().get("mentions");
        }

        if(tweetElement.attributes().hasKey("data-has-parent-tweet")) {
            has_parent_tweet = Boolean.parseBoolean(tweetElement.attributes().get("data-has-parent-tweet"));
        }

        if (tweetElement.attributes().hasKey("data-retweet-id")) {
            retweet_id = tweetElement.attributes().get("data-retweet-id");
            retweeter = tweetElement.attributes().get("data-retweeter");
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
}