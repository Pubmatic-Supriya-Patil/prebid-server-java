package org.prebid.server.bidder.pubmatic.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
public class ExtAlternateBidderCodes {

    private Boolean enabled;

    private Map<String, ExtAlternateBidderCodeConfig> bidders;

    @JsonProperty("enabled")
    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @JsonProperty("bidders")
    public Map<String, ExtAlternateBidderCodeConfig> getBidders() {
        return bidders;
    }

    public void setBidders(Map<String, ExtAlternateBidderCodeConfig> bidders) {
        this.bidders = bidders;
    }
}
