package org.prebid.server.proto.openrtb.ext.request.pubmatic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtImpPubmaticWrapper {

    @JsonProperty("wrap_id")
    private Integer wrapId;

    @JsonProperty("profile_id")
    private Integer profileId;

    public ExtImpPubmaticWrapper() {
    }

    public ExtImpPubmaticWrapper(Integer wrapId, Integer profileId) {
        this.wrapId = wrapId;
        this.profileId = profileId;
    }

    public static ExtImpPubmaticWrapper of(Integer wrapId, Integer profileId) {
        return new ExtImpPubmaticWrapper(wrapId, profileId);
    }

    public Integer getWrapId() {
        return wrapId;
    }

    public Integer getProfileId() {
        return profileId;
    }
}
