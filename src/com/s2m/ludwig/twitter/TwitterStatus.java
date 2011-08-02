package com.s2m.ludwig.twitter;

/*************************************************************************
 * 
 *  Description
 *  -------
 *  
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.codehaus.jackson.JsonNode;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
public class TwitterStatus {


    public final long id;

    public final Date createdAt;

    public final String source;

    public final String text;

    public final boolean truncated;

    public final Long inReplyToStatusId;

    public final Long inReplyToUserId;

    public final String inReplyToScreenName;

    public final boolean favorited;

    public final boolean retweeted;

    public final int retweetCount;

    public final TwitterUser user;

    /**
     * @throws ParseException
     */
    public TwitterStatus(JsonNode json) throws Exception {
        id = json.path("id").getLongValue();
        createdAt = createdAtDateFormat().parse(json.path("created_at").getTextValue());

        source = json.path("source").getTextValue();
        text = json.path("text").getTextValue();
        truncated = json.path("truncated").getBooleanValue();

        inReplyToStatusId = json.path("in_reply_to_status_id").isNull() ? null : json.path("in_reply_to_status_id").getLongValue();
        inReplyToUserId = json.path("in_reply_to_user_id").isNull() ? null : json.path("in_reply_to_user_id").getLongValue();
        inReplyToScreenName = json.path("in_reply_to_screen_name").isNull() ? null : json.path("in_reply_to_screen_name").getTextValue();

        favorited = json.path("favorited").getBooleanValue();
        retweeted = json.path("retweeted").getBooleanValue();
        retweetCount = json.path("retweet_count").getIntValue();

        this.user = new TwitterUser(json.path("user"));
    }

    /**
     * @return
     */
    private SimpleDateFormat createdAtDateFormat() {
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    }

}
