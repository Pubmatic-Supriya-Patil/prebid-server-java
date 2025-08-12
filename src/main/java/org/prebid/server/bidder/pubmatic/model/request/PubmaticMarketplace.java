package org.prebid.server.bidder.pubmatic.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class PubmaticMarketplace {

    @JsonProperty("allowedbidders")
    List<String> allowedBidders;
}
