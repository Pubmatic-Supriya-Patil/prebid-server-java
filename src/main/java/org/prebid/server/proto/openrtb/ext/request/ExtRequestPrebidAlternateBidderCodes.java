package org.prebid.server.proto.openrtb.ext.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.auction.aliases.AlternateBidderCodesConfig;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtRequestPrebidAlternateBidderCodes implements AlternateBidderCodesConfig {

    private Boolean enabled;
    private Map<String, ExtRequestPrebidAlternateBidderCodesBidder> bidders;

    public static ExtRequestPrebidAlternateBidderCodes of(Boolean enabled,
            Map<String, ExtRequestPrebidAlternateBidderCodesBidder> bidders) {
        return new ExtRequestPrebidAlternateBidderCodes(enabled, bidders);
    }
}
