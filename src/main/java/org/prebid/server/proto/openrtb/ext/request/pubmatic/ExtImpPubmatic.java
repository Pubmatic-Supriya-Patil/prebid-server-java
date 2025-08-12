package org.prebid.server.proto.openrtb.ext.request.pubmatic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticWrapper;

import java.util.List;

/**
 * Defines the contract for bidrequest.imp[i].ext.pubmatic
 * PublisherId and adSlot are mandatory parameters, others are optional parameters
 * Keywords is bid specific parameter,
 * WrapExt needs to be sent once per bid request
 */
@Builder
@Data
@AllArgsConstructor(staticName = "of")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ExtImpPubmatic {

    @JsonProperty("publisherId")
    private String publisherId;

    @JsonProperty("adSlot")
    String adSlot;

    String dctr;

    @JsonProperty("pmzoneid")
    String pmZoneId;

    private PubmaticWrapper wrapper;

    List<ExtImpPubmaticKeyVal> keywords;

    String kadfloor;
}
