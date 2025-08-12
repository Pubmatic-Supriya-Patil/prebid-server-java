package org.prebid.server.bidder.pubmatic.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.prebid.server.proto.openrtb.ext.request.pubmatic.ExtImpPubmatic;

@Data
@AllArgsConstructor(staticName = "of")
public class PubmaticBidderImpExt {

    ExtImpPubmatic bidder;

    ObjectNode data;

    Integer ae;

    @JsonProperty("gpid")
    String gpId;
}
