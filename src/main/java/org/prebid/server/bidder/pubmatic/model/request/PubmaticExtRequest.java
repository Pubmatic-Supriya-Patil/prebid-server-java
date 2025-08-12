package org.prebid.server.bidder.pubmatic.model.request;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidAlternateBidderCodes;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidAlternateBidderCodesBidder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubmaticExtRequest {

    private PubmaticWrapper wrapper;
    private List<String> acat;
    private PubmaticMarketplace marketplace;

    @JsonProperty("prebid")
    private ExtRequestPrebid prebid;

    private final Map<String, JsonNode> additionalProperties = new HashMap<>();

    public PubmaticWrapper getWrapper() {
        return wrapper;
    }

    public void setWrapper(PubmaticWrapper wrapper) {
        this.wrapper = wrapper;
    }

    public List<String> getAcat() {
        return acat;
    }

    public void setAcat(List<String> acat) {
        this.acat = acat;
    }

    public PubmaticMarketplace getMarketplace() {
        return marketplace;
    }

    public void setMarketplace(PubmaticMarketplace marketplace) {
        this.marketplace = marketplace;
    }

    public void setPrebid(ExtRequestPrebid prebid) {
        this.prebid = prebid;
    }

    public ExtRequestPrebid getPrebid() {
        return prebid;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String key, JsonNode value) {
        additionalProperties.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public List<String> resolveAllowedBidderCodes(String bidder) {
        if (prebid == null || prebid.getAlternateBidderCodes() == null) {
            return null;
        }

        final ExtRequestPrebidAlternateBidderCodes alternateCodes = prebid.getAlternateBidderCodes();
        final ExtRequestPrebidAlternateBidderCodesBidder config = alternateCodes.getBidders().get(bidder);

        if (!Boolean.TRUE.equals(alternateCodes.getEnabled())
                || config == null || !Boolean.TRUE.equals(config.getEnabled())) {
            return null;
        }

        final List<String> codes = new ArrayList<>(config.getAllowedBidderCodes());
        if (codes == null || (codes.size() == 1 && "*".equals(codes.get(0)))) {
            return List.of("all");
        }

        final List<String> result = new ArrayList<>();
        result.add(bidder); // Always include current bidder
        result.addAll(codes);
        return result;
    }

}
