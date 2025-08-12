package org.prebid.server.bidder.pubmatic.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ExtAlternateBidderCodeConfig {

    private Boolean enabled;

    @JsonProperty("allowedbiddercodes")
    private List<String> allowedBidderCodes;

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getAllowedBidderCodes() {
        return allowedBidderCodes;
    }

    public void setAllowedBidderCodes(List<String> allowedBidderCodes) {
        this.allowedBidderCodes = allowedBidderCodes;
    }
}
