package com.s2m.ludwig.twitter;




import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.codehaus.jackson.JsonNode;


public class TwitterUser {


    public final long id;

    public final String name;

    public final String screenName;

    public final Date createdAt;

    public final String description;

    public final String url;

    public final String lang;

    public final String location;

    public final String timeZone;

    public final Integer utcOffset;

    public final int statusesCount;

    public final int favouritesCount;

    public final int followersCount;

    public final int friendsCount;

    public final int listedCount;

    public final String profileImageUrl;

    public final String profileBackgroundImageUrl;

    public final String profileTextColor;

    public final String profileLinkColor;

    public final String profileSidebarFillColor;

    public final String profileSidebarBorderColor;

    public final String profileBackgroundColor;

    public final boolean profileBackgroundTile;

    public final boolean profileUseBackgroundImage;

    public final boolean isProtected;

    public final boolean following;

    public final boolean followRequestSent;

    public final boolean notifications;

    public final boolean verified;

    public final boolean geoEnabled;

    public final boolean contributorsEnabled;

    public final boolean showAllInlineMedia;

    public final boolean isTranslator;

    /**
     * @param path
     * @throws Exception
     */
    public TwitterUser(JsonNode json) throws Exception {
        id = json.path("id").getLongValue();
        name = json.path("name").getTextValue();
        screenName = json.path("screen_name").getTextValue();
        createdAt = createdAtDateFormat().parse(json.path("created_at").getTextValue());
        description = json.path("description").getTextValue();
        url = json.path("url").getTextValue();

        lang = json.path("lang").getTextValue();
        location = json.path("location").getTextValue();
        timeZone = json.path("time_zone").getTextValue();
        utcOffset = json.path("utc_offset").isNumber() ? json.path("utc_offset").getIntValue() : null;

        statusesCount = json.path("statuses_count").getIntValue();
        favouritesCount = json.path("favourites_count").getIntValue();
        followersCount = json.path("followers_count").getIntValue();
        friendsCount = json.path("friends_count").getIntValue();
        listedCount = json.path("listed_count").getIntValue();

        profileImageUrl = json.path("profile_image_url").getTextValue();
        profileBackgroundImageUrl = json.path("profile_background_image_url").getTextValue();
        profileTextColor = json.path("profile_text_color").getTextValue();
        profileLinkColor = json.path("profile_link_color").getTextValue();
        profileSidebarFillColor = json.path("profile_sidebar_fill_color").getTextValue();
        profileSidebarBorderColor = json.path("profile_sidebar_border_color").getTextValue();
        profileBackgroundColor = json.path("profile_background_color").getTextValue();
        profileBackgroundTile = json.path("profile_background_tile").getBooleanValue();
        profileUseBackgroundImage = json.path("profile_use_background_image").getBooleanValue();

        isProtected = json.path("protected").getBooleanValue();
        following = json.path("following").getBooleanValue();
        followRequestSent = json.path("follow_request_sent").getBooleanValue();

        notifications = json.path("notifications").getBooleanValue();
        verified = json.path("verified").getBooleanValue();
        geoEnabled = json.path("geo_enabled").getBooleanValue();
        contributorsEnabled = json.path("contributors_enabled").getBooleanValue();
        showAllInlineMedia = json.path("show_all_inline_media").getBooleanValue();
        isTranslator = json.path("is_translator").getBooleanValue();
    }

    /**
     * @return
     */
    private SimpleDateFormat createdAtDateFormat() {
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    }

}
