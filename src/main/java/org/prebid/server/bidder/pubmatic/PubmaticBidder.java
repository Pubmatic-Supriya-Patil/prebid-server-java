package org.prebid.server.bidder.pubmatic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.SeatBid;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.CompositeBidderResponse;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticBidderImpExt;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticExtDataAdServer;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticExtRequest;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticMarketplace;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticWrapper;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticBidExt;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticBidResponse;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticExtBidResponse;
import org.prebid.server.bidder.pubmatic.model.response.VideoCreativeInfo;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.FlexibleExtension;
import org.prebid.server.proto.openrtb.ext.request.ExtApp;
import org.prebid.server.proto.openrtb.ext.request.ExtAppPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidAlternateBidderCodes;
import org.prebid.server.proto.openrtb.ext.request.pubmatic.ExtImpPubmatic;
import org.prebid.server.proto.openrtb.ext.request.pubmatic.ExtImpPubmaticKeyVal;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidMeta;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidVideo;
import org.prebid.server.proto.openrtb.ext.response.ExtIgi;
import org.prebid.server.proto.openrtb.ext.response.ExtIgiIgs;
import org.prebid.server.util.BidderUtil;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.StreamUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PubmaticBidder implements Bidder<BidRequest> {

    private static final String DCTR_KEY_NAME = "key_val";
    private static final String PM_ZONE_ID_KEY_NAME = "pmZoneId";
    private static final String PM_ZONE_ID_OLD_KEY_NAME = "pmZoneID";
    private static final String IMP_EXT_AD_UNIT_KEY = "dfp_ad_unit_code";
    private static final String AD_SERVER_GAM = "gam";
    private static final String PREBID = "prebid";
    private static final String ACAT_EXT_REQUEST = "acat";
    private static final String WRAPPER_EXT_REQUEST = "wrapper";
    private static final String BIDDER_NAME = "pubmatic";
    private static final String AE = "ae";
    private static final String GP_ID = "gpid";
    private static final String IMP_EXT_PBADSLOT = "pbadslot";
    private static final String IMP_EXT_ADSERVER = "adserver";
    private static final List<String> IMP_EXT_DATA_RESERVED_FIELD = List.of(IMP_EXT_PBADSLOT, IMP_EXT_ADSERVER);
    private static final String DCTR_VALUE_FORMAT = "%s=%s";

    private final String endpointUrl;
    private final JacksonMapper mapper;

    public PubmaticBidder(String endpointUrl, JacksonMapper mapper) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl));
        this.mapper = Objects.requireNonNull(mapper);
    }

    private PubmaticExtRequest extractPubmaticExtFromRequest(BidRequest request) {
        if (request == null || request.getExt() == null || request.getExt().getPrebid() == null) {
            return new PubmaticExtRequest();
        }

        try {
            // final JsonNode bidderParams = mapper.mapper().convertValue(
            //         request.getExt().getPrebid().getBidderparams(), JsonNode.class);
            final JsonNode bidderParams = getExtRequestPrebidBidderparams(request);
            final PubmaticExtRequest extRequest = new PubmaticExtRequest();

            // If bidderParams is null, skip wrapper and acat extraction but continue with marketplace
            if (bidderParams != null) {
                // Extract wrapper
                final JsonNode wrapperNode = bidderParams.get(WRAPPER_EXT_REQUEST);
                if (wrapperNode != null && !wrapperNode.isNull()) {
                    extRequest.setWrapper(extractWrapper(wrapperNode));
                }

                // Extract acat
                final JsonNode acatNode = bidderParams.get(ACAT_EXT_REQUEST);
                if (acatNode != null && acatNode.isArray()) {
                    extRequest.setAcat(extractAcat(acatNode));
                }
            }

            // Extract marketplace (always execute regardless of bidderParams)
            if (request.getExt() != null && request.getExt().getPrebid() != null) {
                extRequest.setPrebid(request.getExt().getPrebid());
                final List<String> allowedBidders = extRequest.resolveAllowedBidderCodes("pubmatic");
                if (allowedBidders != null) {
                    extRequest.setMarketplace(extractMarketplace(allowedBidders));
                }
            }

            return extRequest;

        } catch (IllegalArgumentException e) {
            throw new PreBidException("Error converting bidder params in Pubmatic extension: " + e.getMessage());
        }
    }

    private PubmaticWrapper extractWrapper(JsonNode wrapperNode) {
        final PubmaticWrapper wrapper = PubmaticWrapper.of(0, 0);
        if (wrapperNode.has("profile")) {
            wrapper.setProfile(extractIntValue(wrapperNode.get("profile")));
        }
        if (wrapperNode.has("version")) {
            wrapper.setVersion(extractIntValue(wrapperNode.get("version")));
        }
        return wrapper;
    }

    private List<String> extractAcat(JsonNode acatNode) {
        return Arrays.stream(mapper.mapper().convertValue(acatNode, String[].class))
                .map(StringUtils::stripToEmpty)
                .toList();
    }

    private PubmaticMarketplace extractMarketplace(List<String> allowedBidders) {
        return PubmaticMarketplace.builder()
                .allowedBidders(allowedBidders)
                .build();
    }

    private int extractIntValue(JsonNode node) {
        if (node == null) {
            return 0;
        } else if (node.isTextual()) {
            try {
                return Integer.parseInt(node.textValue());
            } catch (NumberFormatException e) {
                throw e;
            }
        } else if (node.isInt()) {
            return node.intValue();
        }
        return 0;
    }

    private static JsonNode getExtRequestPrebidBidderparams(BidRequest request) {
        final ExtRequest extRequest = request.getExt();
        final ExtRequestPrebid extRequestPrebid = extRequest != null ? extRequest.getPrebid() : null;
        final ObjectNode bidderParams = extRequestPrebid != null ? extRequestPrebid.getBidderparams() : null;
        return bidderParams != null ? bidderParams.get(BIDDER_NAME) : null;
    }

    @Override
    public Result<List<HttpRequest<BidRequest>>> makeHttpRequests(BidRequest request) {
        final List<BidderError> errors = new ArrayList<>();
        final List<Imp> validImps = new ArrayList<>();
        final Pair<String, String> displayManagerFields;
        final List<String> acat = new ArrayList<>();

        displayManagerFields = extractDisplayManagerFields(request.getApp());
        String publisherId = null;
        PubmaticWrapper wrapper = null;
        boolean extractWrapperExtFromImp = true;
        boolean extractPubIDFromImp = true;

        // First try to extract from request ext
        try {
            final PubmaticExtRequest extRequest = extractPubmaticExtFromRequest(request);
            acat.clear();
            acat.addAll(ObjectUtils.defaultIfNull(extRequest.getAcat(), Collections.emptyList()));

            if (extRequest != null && extRequest.getWrapper() != null) {
                final PubmaticWrapper requestWrapper = extRequest.getWrapper();
                if (requestWrapper != null) {
                    // Always use the wrapper from request ext if it exists
                    wrapper = requestWrapper;
                    extractWrapperExtFromImp = false;
                }
            }
            if (extractWrapperExtFromImp) {
                if (request.getImp() == null || request.getImp().isEmpty()) {
                    return Result.of(Collections.emptyList(), errors);
                }

                for (Imp imp : request.getImp()) {
                    final PubmaticBidderImpExt impExt = parseImpExt(imp);
                    validateMediaType(imp);
                    if (impExt != null) {
                        final ExtImpPubmatic extImpPubmatic = impExt.getBidder();
                        if (extImpPubmatic != null) {
                            final PubmaticWrapper wrapperExtFromImp = extImpPubmatic.getWrapper();
                            if (wrapperExtFromImp != null) {
                                if (wrapper == null) {
                                    wrapper = PubmaticWrapper.of(0, 0);
                                }
                                if (wrapper.getProfile() == 0) {
                                    wrapper.setProfile(wrapperExtFromImp.getProfile());
                                }
                                if (wrapper.getVersion() == 0) {
                                    wrapper.setVersion(wrapperExtFromImp.getVersion());
                                }
                            }
                            if (extractPubIDFromImp && StringUtils.isNotBlank(extImpPubmatic.getPublisherId())) {
                                publisherId = extImpPubmatic.getPublisherId();
                                extractPubIDFromImp = false;
                            }
                            if (wrapper != null) {
                                impExt.getBidder().setWrapper(wrapper);
                            }
                            if (publisherId != null) {
                                impExt.getBidder().setPublisherId(publisherId);
                            }
                        }
                        validImps.add(modifyImp(imp, impExt, displayManagerFields.getLeft(),
                                displayManagerFields.getRight()));
                    }
                }
            }
        } catch (Exception e) {
            errors.add(BidderError.badInput(e.getMessage()));
        }
        if (!extractWrapperExtFromImp) {
            for (Imp imp : request.getImp()) {
                validateMediaType(imp);
            }
            validImps.addAll(request.getImp());

        }
        if (validImps.isEmpty()) {
            return Result.withErrors(errors);
        }

        final BidRequest outgoingRequest = modifyBidRequest(request, validImps, publisherId, wrapper, acat);
        return Result.of(Collections.singletonList(makeHttpRequest(outgoingRequest)), errors);
    }
    // @Override
    // public Result<List<HttpRequest<BidRequest>>> makeHttpRequests(BidRequest request) {
    //     final List<BidderError> errors = new ArrayList<>();
    //     final List<Imp> validImps = new ArrayList<>();
    //     final Pair<String, String> displayManagerFields = extractDisplayManagerFields(request.getApp());
    //     final List<String> acat = new ArrayList<>();

    //     String publisherId = null;
    //     PubmaticWrapper wrapper = null;
    //     boolean extractWrapperExtFromImp = true;
    //     boolean extractPubIDFromImp = true;

    //     try {
    //         final PubmaticExtRequest extRequest = extractPubmaticExtFromRequest(request);
    //         acat.clear();
    //         acat.addAll(ObjectUtils.defaultIfNull(extRequest.getAcat(), Collections.emptyList()));

    //         if (extRequest != null && extRequest.getWrapper() != null) {
    //             // Always use the wrapper from request ext if it exists
    //             wrapper = extRequest.getWrapper();
    //             extractWrapperExtFromImp = false;
    //         }

    //         if (extractWrapperExtFromImp) {
    //             // Process imps only if needed
    //             if (request.getImp() == null || request.getImp().isEmpty()) {
    //                 return Result.of(Collections.emptyList(), errors);
    //             }

    //             for (Imp imp : request.getImp()) {
    //                 final PubmaticBidderImpExt impExt = parseImpExt(imp);
    //                 validateMediaType(imp);

    //                 if (impExt != null) {
    //                     final ExtImpPubmatic extImpPubmatic = impExt.getBidder();
    //                     if (extImpPubmatic != null) {
    //                         final PubmaticWrapper wrapperExtFromImp = extImpPubmatic.getWrapper();

    //                         if (wrapperExtFromImp != null) {
    //                             if (wrapper == null) {
    //                                 wrapper = PubmaticWrapper.of(0, 0);
    //                             }
    //                             if (wrapper.getProfile() == 0) {
    //                                 wrapper.setProfile(wrapperExtFromImp.getProfile());
    //                             }
    //                             if (wrapper.getVersion() == 0) {
    //                                 wrapper.setVersion(wrapperExtFromImp.getVersion());
    //                             }
    //                         }

    //                         if (extractPubIDFromImp && StringUtils.isNotBlank(extImpPubmatic.getPublisherId())) {
    //                             publisherId = extImpPubmatic.getPublisherId();
    //                             extractPubIDFromImp = false;
    //                         }

    //                         if (wrapper != null) {
    //                             impExt.getBidder().setWrapper(wrapper);
    //                         }
    //                         if (publisherId != null) {
    //                             impExt.getBidder().setPublisherId(publisherId);
    //                         }
    //                     }

    //                     validImps.add(modifyImp(imp, impExt, displayManagerFields.getLeft(),
    //                             displayManagerFields.getRight()));
    //                 }
    //             }

    //             // Only enforce this empty-check if we actually processed imps
    //             if (validImps.isEmpty()) {
    //                 return Result.withErrors(errors);
    //             }
    //         } else {
    //             // If wrapper came from request ext, just take the imps as-is
    //             validImps.addAll(request.getImp());
    //         }

    //     } catch (Exception e) {
    //         errors.add(BidderError.badInput(e.getMessage()));
    //     }

    //     final BidRequest outgoingRequest = modifyBidRequest(request, validImps, publisherId, wrapper, acat);
    //     return Result.of(Collections.singletonList(makeHttpRequest(outgoingRequest)), errors);
    // }

    private static String getPropertyValue(FlexibleExtension flexibleExtension, String propertyName) {
        return Optional.ofNullable(flexibleExtension)
                .map(ext -> ext.getProperty(propertyName))
                .filter(JsonNode::isValueNode)
                .map(JsonNode::asText)
                .orElse(null);
    }

    private Pair<String, String> extractDisplayManagerFields(App app) {
        String source;
        String version;

        final ExtApp extApp = app != null ? app.getExt() : null;
        final ExtAppPrebid extAppPrebid = extApp != null ? extApp.getPrebid() : null;

        source = extAppPrebid != null ? extAppPrebid.getSource() : null;
        version = extAppPrebid != null ? extAppPrebid.getVersion() : null;
        if (StringUtils.isNoneBlank(source, version)) {
            return Pair.of(source, version);
        }

        source = getPropertyValue(extApp, "source");
        version = getPropertyValue(extApp, "version");
        return StringUtils.isNoneBlank(source, version)
                ? Pair.of(source, version)
                : Pair.of(null, null);
    }

    private String getDisplayManager(App app) {
        return Optional.ofNullable(app)
                .map(App::getExt)
                .map(ExtApp::getPrebid)
                .map(ExtAppPrebid::getSource)
                .orElse(null);
    }

    private void validateMediaType(Imp imp) {
        if (imp.getBanner() == null && imp.getVideo() == null && imp.getXNative() == null) {
            throw new PreBidException(
                    "Invalid MediaType. PubMatic only supports Banner, Video and Native. Ignoring ImpID=%s"
                            .formatted(imp.getId()));
        }
    }

    private boolean isValidBanner(Banner banner) {
        return banner != null
                && (banner.getW() != null
                || banner.getH() != null
                || (banner.getFormat() != null && !banner.getFormat().isEmpty()));
    }

    private PubmaticBidderImpExt parseImpExt(Imp imp) {
        try {
            return mapper.mapper().convertValue(imp.getExt(), PubmaticBidderImpExt.class);
        } catch (IllegalArgumentException e) {
            throw new PreBidException(e.getMessage());
        }
    }

    private boolean isWrapperValid(PubmaticWrapper wrapper) {
        return wrapper != null
                && wrapper.getProfile() != null
                && wrapper.getVersion() != null;
    }

    private static Integer stripToNull(Integer value) {
        return value == null || value == 0 ? null : value;
    }

    private ObjectNode makeKeywords(PubmaticBidderImpExt impExt) {
        final ObjectNode keywordsNode = mapper.mapper().createObjectNode();

        final ExtImpPubmatic extBidder = impExt.getBidder();
        putExtBidderKeywords(keywordsNode, extBidder);
        putExtDataKeywords(keywordsNode, impExt.getData(), extBidder.getDctr());

        if (impExt.getAe() != null) {
            keywordsNode.put(AE, impExt.getAe());
        }
        if (impExt.getGpId() != null) {
            keywordsNode.put(GP_ID, impExt.getGpId());
        }

        return keywordsNode;
    }

    private static void putExtBidderKeywords(ObjectNode keywords, ExtImpPubmatic extBidder) {
        for (ExtImpPubmaticKeyVal keyword : CollectionUtils.emptyIfNull(extBidder.getKeywords())) {
            if (CollectionUtils.isEmpty(keyword.getValue())) {
                continue;
            }
            keywords.put(keyword.getKey(), String.join(",", keyword.getValue()));
        }

        final JsonNode pmZoneIdKeyWords = keywords.remove(PM_ZONE_ID_OLD_KEY_NAME);
        final String pmZomeId = extBidder.getPmZoneId();
        if (StringUtils.isNotEmpty(pmZomeId)) {
            keywords.put(PM_ZONE_ID_KEY_NAME, pmZomeId);
        } else if (pmZoneIdKeyWords != null) {
            keywords.set(PM_ZONE_ID_KEY_NAME, pmZoneIdKeyWords);
        }
    }

    private void putExtDataKeywords(ObjectNode keywords, ObjectNode extData, String dctr) {
        final String newDctr = extractDctr(dctr, extData);
        if (StringUtils.isNotEmpty(newDctr)) {
            keywords.put(DCTR_KEY_NAME, newDctr);
        }

        final String adUnitCode = extractAdUnitCode(extData);
        if (StringUtils.isNotEmpty(adUnitCode)) {
            keywords.put(IMP_EXT_AD_UNIT_KEY, adUnitCode);
        }
    }

    private static String extractDctr(String firstDctr, ObjectNode extData) {
        if (extData == null) {
            return firstDctr;
        }

        return Stream.concat(
                        Stream.of(firstDctr),
                        StreamUtil.asStream(extData.fields())
                                .filter(entry -> !IMP_EXT_DATA_RESERVED_FIELD.contains(entry.getKey()))
                                .map(PubmaticBidder::buildDctrPart))
                .filter(Objects::nonNull)
                .collect(Collectors.joining("|"));
    }

    private static String buildDctrPart(Map.Entry<String, JsonNode> dctrPart) {
        final JsonNode value = dctrPart.getValue();
        final String valueAsString = value.isValueNode()
                ? StringUtils.trim(value.asText())
                : null;
        final String arrayAsString = valueAsString == null && value.isArray()
                ? StreamUtil.asStream(value.elements())
                .map(JsonNode::asText)
                .map(StringUtils::trim)
                .collect(Collectors.joining(","))
                : null;

        final String valuePart = ObjectUtils.firstNonNull(valueAsString, arrayAsString);

        return valuePart != null
                ? DCTR_VALUE_FORMAT.formatted(StringUtils.trim(dctrPart.getKey()), valuePart)
                : null;
    }

    private String extractAdUnitCode(ObjectNode extData) {
        if (extData == null) {
            return null;
        }

        final PubmaticExtDataAdServer extAdServer = extractAdServer(extData);
        final String adServerName = extAdServer != null ? extAdServer.getName() : null;
        final String adServerAdSlot = extAdServer != null ? extAdServer.getAdSlot() : null;

        return AD_SERVER_GAM.equals(adServerName) && StringUtils.isNotEmpty(adServerAdSlot)
                ? adServerAdSlot
                : Optional.ofNullable(extData.get(IMP_EXT_PBADSLOT))
                .map(JsonNode::asText)
                .orElse(null);
    }

    private PubmaticExtDataAdServer extractAdServer(ObjectNode extData) {
        try {
            return mapper.mapper().treeToValue(extData.get(IMP_EXT_ADSERVER), PubmaticExtDataAdServer.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private Imp modifyImp(Imp imp, PubmaticBidderImpExt impExt, String displayManager,
                           String displayManagerVersion) {

        final Banner banner = imp.getBanner();
        final ExtImpPubmatic impExtBidder = impExt.getBidder();

        final ObjectNode newExt = makeKeywords(impExt);

        final Imp.ImpBuilder impBuilder = imp.toBuilder()
                .banner(isValidBanner(banner) ? assignSizesIfMissing(banner) : null)
                .audio(null)
                .bidfloor(resolveBidFloor(impExtBidder.getKadfloor(), imp.getBidfloor()))
                .displaymanager(StringUtils.firstNonBlank(imp.getDisplaymanager(), displayManager))
                .displaymanagerver(StringUtils.firstNonBlank(imp.getDisplaymanagerver(), displayManagerVersion))
                .ext(!newExt.isEmpty() ? newExt : null);

        enrichWithAdSlotParameters(impBuilder, impExtBidder.getAdSlot(), banner);

        return impBuilder.build();
    }

    private static void enrichWithAdSlotParameters(Imp.ImpBuilder impBuilder, String adSlot, Banner banner) {
        final String trimmedAdSlot = StringUtils.trimToNull(adSlot);
        if (StringUtils.isEmpty(trimmedAdSlot)) {
            return;
        }

        if (!trimmedAdSlot.contains("@")) {
            impBuilder.tagid(trimmedAdSlot);
            return;
        }

        final String[] adSlotParams = trimmedAdSlot.split("@");
        final String trimmedParam0 = adSlotParams.length == 2 ? adSlotParams[0].trim() : null;
        final String trimmedParam1 = adSlotParams.length == 2 ? adSlotParams[1].trim() : null;

        if (adSlotParams.length != 2
                || StringUtils.isEmpty(trimmedParam0)
                || StringUtils.isEmpty(trimmedParam1)) {

            throw new PreBidException("Invalid adSlot '%s'".formatted(trimmedAdSlot));
        }

        impBuilder.tagid(trimmedParam0);
        if (banner == null) {
            return;
        }
        final String[] adSize = trimmedParam1.toLowerCase().split("x");
        if (adSize.length != 2) {
            throw new PreBidException("Invalid size provided in adSlot '%s'".formatted(trimmedAdSlot));
        }

        final Integer width = parseAdSizeParam(adSize[0], "width", adSlot);

        final String[] heightParams = adSize[1].split(":");
        final Integer height = parseAdSizeParam(heightParams[0], "height", adSlot);

        impBuilder.banner(modifyWithSizeParams(banner, width, height));
    }

    private static Integer parseAdSizeParam(String number, String paramName, String adSlot) {
        try {
            return Integer.parseInt(number.trim());
        } catch (NumberFormatException e) {
            throw new PreBidException("Invalid %s provided in adSlot '%s'".formatted(paramName, adSlot));
        }
    }

    private Banner assignSizesIfMissing(Banner banner) {
        if (banner.getW() != null && banner.getH() != null) {
            return banner;
        }

        final List<Format> format = banner.getFormat();
        if (CollectionUtils.isEmpty(format)) {
            throw new PreBidException("Banner width and height is not provided, but required");
        }

        final Format firstFormat = format.getFirst();
        return banner.toBuilder().w(firstFormat.getW()).h(firstFormat.getH()).build();
        // final Format firstFormat = format.getFirst();
        // return modifyWithSizeParams(banner, firstFormat.getW(), firstFormat.getH());
    }

    private static Banner modifyWithSizeParams(Banner banner, Integer width, Integer height) {
        return banner.toBuilder()
                .w(stripToNull(width))
                .h(stripToNull(height))
                .build();
    }

    private BigDecimal resolveBidFloor(String kadfloor, BigDecimal existingFloor) {
        final BigDecimal kadFloor = parseKadFloor(kadfloor);
        return ObjectUtils.allNotNull(kadFloor, existingFloor)
                ? kadFloor.max(existingFloor)
                : ObjectUtils.firstNonNull(kadFloor, existingFloor);
    }

    private static BigDecimal parseFloor(String floor) {
        if (floor == null) {
            return null;
        }
        try {
            return new BigDecimal(floor);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static BigDecimal parseKadFloor(String kadFloorString) {
        if (StringUtils.isBlank(kadFloorString)) {
            return null;
        }
        try {
            return new BigDecimal(StringUtils.trimToEmpty(kadFloorString));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Pair<Integer, Integer> getAdSlotWidthAndHeight(String adSlot) {
        final String[] adSlotParts = adSlot.split("@");
        if (adSlotParts.length != 2 || StringUtils.isAnyBlank(adSlotParts[0], adSlotParts[1])) {
            throw new PreBidException("Invalid adSlot '%s'".formatted(adSlot));
        }

        final String[] adSize = adSlotParts[1].split("x");
        if (adSize.length != 2 || StringUtils.isAnyBlank(adSize[0], adSize[1])) {
            throw new PreBidException("Invalid adSlot '%s'".formatted(adSlot));
        }

        final Integer width = parseAdSlotDimension(adSize[0], "width", adSlot);
        final Integer height = parseAdSlotDimension(adSize[1], "height", adSlot);

        return Pair.of(width, height);
    }

    private Integer parseAdSlotDimension(String dimension, String paramName, String adSlot) {
        try {
            return Integer.parseInt(dimension);
        } catch (NumberFormatException e) {
            throw new PreBidException("Invalid %s provided in adSlot '%s'".formatted(paramName, adSlot));
        }
    }

    private BidRequest modifyBidRequest(BidRequest request,
                                         List<Imp> imps,
                                         String publisherId,
                                         PubmaticWrapper wrapper,
                                         List<String> acat) {

        return request.toBuilder()
                .imp(imps)
                .site(modifySite(request.getSite(), publisherId))
                .app(modifyApp(request.getApp(), publisherId))
                .ext(modifyExtRequest(request, imps, acat, wrapper))
                .build();
    }

    private Site modifySite(Site site, String publisherId) {
        return publisherId != null && site != null
                ? site.toBuilder()
                .publisher(modifyPublisher(site.getPublisher(), publisherId))
                .build()
                : site;
    }

    private App modifyApp(App app, String publisherId) {
        return publisherId != null && app != null
                ? app.toBuilder()
                .publisher(modifyPublisher(app.getPublisher(), publisherId))
                .build()
                : app;
    }

    private static Publisher modifyPublisher(Publisher publisher, String publisherId) {
        return publisher != null
                ? publisher.toBuilder().id(publisherId).build()
                : Publisher.builder().id(publisherId).build();
    }

    private ExtRequest modifyExtRequest(BidRequest request,
                                         List<Imp> imps,
                                         List<String> acat,
                                         PubmaticWrapper wrapper) {

        final List<String> sanitizedAcat = acat == null ? Collections.emptyList()
                : acat.stream()
                .map(StringUtils::trimToNull)
                .filter(Objects::nonNull)
                .toList();

        // Create the additional properties for the ExtRequest
        final ObjectNode additionalProperties = mapper.mapper().createObjectNode();

        // Set wrapper as additional property if valid
        if (isWrapperValid(wrapper)) {
            additionalProperties.set(WRAPPER_EXT_REQUEST, mapper.mapper().valueToTree(wrapper));
        }

        // Set acat as additional property if present
        if (CollectionUtils.isNotEmpty(sanitizedAcat)) {
            additionalProperties.set(ACAT_EXT_REQUEST, mapper.mapper().valueToTree(sanitizedAcat));
        }

        final ExtRequest originalExt = request.getExt();
        final ExtRequestPrebid originalPrebid = originalExt != null ? originalExt.getPrebid() : null;
        // Preserve alternatebiddercodes if present in original ext
        final ExtRequestPrebidAlternateBidderCodes alternateBidderCodes = originalPrebid != null
                ? originalPrebid.getAlternateBidderCodes()
                : null;

        // Create bidder params for prebid section
        ExtRequestPrebid prebid = null;
        final ObjectNode pubmaticParams = mapper.mapper().createObjectNode();

        if (isWrapperValid(wrapper)) {
            pubmaticParams.set(WRAPPER_EXT_REQUEST, mapper.mapper().valueToTree(wrapper));
        }
        if (CollectionUtils.isNotEmpty(sanitizedAcat)) {
            pubmaticParams.set(ACAT_EXT_REQUEST, mapper.mapper().valueToTree(sanitizedAcat));
        }

        if (!pubmaticParams.isEmpty()) {
            final ObjectNode bidderParams = mapper.mapper().createObjectNode();
            // Set wrapper directly in bidder params instead of under "pubmatic"
            if (pubmaticParams.has(WRAPPER_EXT_REQUEST)) {
                bidderParams.set(WRAPPER_EXT_REQUEST, pubmaticParams.get(WRAPPER_EXT_REQUEST));
            }
            if (pubmaticParams.has(ACAT_EXT_REQUEST)) {
                bidderParams.set(ACAT_EXT_REQUEST, pubmaticParams.get(ACAT_EXT_REQUEST));
            }

            final ExtRequestPrebid.ExtRequestPrebidBuilder prebidBuilder = ExtRequestPrebid.builder()
                    .bidderparams(bidderParams)
                    .alternateBidderCodes(alternateBidderCodes);

            prebid = prebidBuilder.build();
        } else if (alternateBidderCodes != null) {
            // If there are no pubmatic params but we have alternateBidderCodes,
            // create a minimal prebid object with just the alternateBidderCodes
            prebid = ExtRequestPrebid.builder()
                    .alternateBidderCodes(alternateBidderCodes)
                    .build();
        }

        // Set alternateBidderCodes in the marketplace object if present
        if (alternateBidderCodes != null) {
            // Create a new marketplace object or use existing one
            ObjectNode marketplace = (ObjectNode) additionalProperties.get("marketplace");
            if (marketplace == null) {
                marketplace = mapper.mapper().createObjectNode();
                additionalProperties.set("marketplace", marketplace);
            }
            // Set alternatebiddercodes in marketplace
            marketplace.set("alternatebiddercodes", mapper.mapper().valueToTree(alternateBidderCodes));
        }

        // Copy other existing properties from original ExtRequest
        if (originalExt != null) {
            final JsonNode originalExtJson = mapper.mapper().valueToTree(originalExt);
            if (originalExtJson.isObject()) {
                final Iterator<Map.Entry<String, JsonNode>> fields = originalExtJson.fields();
                while (fields.hasNext()) {
                    final Map.Entry<String, JsonNode> entry = fields.next();
                    final String key = entry.getKey();
                    // Don't copy prebid, wrapper, acat - we handle these specially
                    if (!Set.of(PREBID, WRAPPER_EXT_REQUEST, ACAT_EXT_REQUEST).contains(key)) {
                        additionalProperties.set(key, entry.getValue());
                    }
                }
            }

            if (originalPrebid != null) {
                if (prebid == null) {
                    prebid = originalPrebid;
                }

                if (alternateBidderCodes != null
                        && MapUtils.isEmpty(alternateBidderCodes.getBidders())) {
                    prebid = prebid.toBuilder().alternateBidderCodes(null).build();
                }
            }
        }

        // Create and return the final ExtRequest
        return mapper.fillExtension(ExtRequest.of(prebid), additionalProperties);
    }

    private HttpRequest<BidRequest> makeHttpRequest(BidRequest request) {
        return BidderUtil.defaultRequest(request, endpointUrl, mapper);
    }

    /**
     * @deprecated for this bidder in favor of @link{makeBidderResponse} which supports additional response data
     */
    @Override
    @Deprecated(forRemoval = true)
    public Result<List<BidderBid>> makeBids(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        return Result.withError(BidderError.generic("Deprecated adapter method invoked"));
    }

    @Override
    public CompositeBidderResponse makeBidderResponse(BidderCall<BidRequest> httpCall, BidRequest bidRequest) {
        try {
            final PubmaticBidResponse bidResponse = mapper.decodeValue(
                    httpCall.getResponse().getBody(), PubmaticBidResponse.class);
            final List<BidderError> errors = new ArrayList<>();

            return CompositeBidderResponse.builder()
                    .bids(extractBids(bidResponse, errors))
                    .igi(extractIgi(bidResponse))
                    .errors(errors)
                    .build();
        } catch (DecodeException e) {
            return CompositeBidderResponse.withError(BidderError.badServerResponse(e.getMessage()));
        }
    }

    private List<BidderBid> extractBids(PubmaticBidResponse bidResponse, List<BidderError> bidderErrors) {
        if (bidResponse == null || CollectionUtils.isEmpty(bidResponse.getSeatbid())) {
            return Collections.emptyList();
        }
        return bidsFromResponse(bidResponse, bidderErrors);
    }

    private List<BidderBid> bidsFromResponse(PubmaticBidResponse bidResponse, List<BidderError> bidderErrors) {
        return bidResponse.getSeatbid().stream()
                .filter(Objects::nonNull)
                .map(SeatBid::getBid)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(bid -> resolveBidderBid(bid, bidResponse.getCur(), bidderErrors))
                .filter(Objects::nonNull)
                .toList();
    }

    private BidderBid resolveBidderBid(Bid bid, String currency, List<BidderError> bidderErrors) {
        final List<String> cat = bid.getCat();
        final List<String> firstCat = CollectionUtils.isNotEmpty(cat)
                ? Collections.singletonList(cat.getFirst())
                : null;

        final PubmaticBidExt pubmaticBidExt = parseBidExt(bid.getExt(), bidderErrors);
        final BidType bidType = getBidType(bid, bidderErrors);

        if (bidType == null) {
            return null;
        }

        final String bidAdm = bid.getAdm();
        final String resolvedAdm = bidAdm != null && bidType == BidType.xNative
                ? resolveNativeAdm(bidAdm, bidderErrors)
                : bidAdm;

        final Bid updatedBid = bid.toBuilder()
                .cat(firstCat)
                .adm(resolvedAdm != null ? resolvedAdm : bidAdm)
                .ext(updateBidExtWithExtPrebid(pubmaticBidExt, bidType, bid.getExt()))
                .build();

        return BidderBid.builder()
                .bid(updatedBid)
                .type(bidType)
                .bidCurrency(currency)
                .dealPriority(getDealPriority(pubmaticBidExt))
                .seat(pubmaticBidExt == null ? null : pubmaticBidExt.getMarketplace())
                .build();
    }

    private PubmaticBidExt parseBidExt(ObjectNode bidExt, List<BidderError> errors) {
        try {
            return bidExt != null ? mapper.mapper().treeToValue(bidExt, PubmaticBidExt.class) : null;
        } catch (JsonProcessingException e) {
            errors.add(BidderError.badServerResponse(e.getMessage()));
            return null;
        }
    }

    private static BidType getBidType(Bid bid, List<BidderError> errors) {
        return switch (bid.getMtype()) {
            case 1 -> BidType.banner;
            case 2 -> BidType.video;
            case 3 -> BidType.audio;
            case 4 -> BidType.xNative;
            case null, default -> {
                errors.add(BidderError.badServerResponse("failed to parse bid mtype (%d) for impression id %s"
                        .formatted(bid.getMtype(), bid.getImpid())));
                yield null;
            }
        };
    }

    private String resolveNativeAdm(String adm, List<BidderError> bidderErrors) {
        final JsonNode admNode;
        try {
            admNode = mapper.mapper().readTree(adm);
        } catch (JsonProcessingException e) {
            bidderErrors.add(BidderError.badServerResponse("Unable to parse native adm: %s".formatted(adm)));
            return null;
        }

        final JsonNode nativeNode = admNode.get("native");
        if (nativeNode != null && !nativeNode.isMissingNode()) {
            return nativeNode.toString();
        }

        return null;
    }

    private ObjectNode updateBidExtWithExtPrebid(PubmaticBidExt pubmaticBidExt, BidType type, ObjectNode extBid) {
        final Integer duration = getDuration(pubmaticBidExt);
        final boolean inBannerVideo = getInBannerVideo(pubmaticBidExt);

        final ExtBidPrebid extBidPrebid = ExtBidPrebid.builder()
                .video(duration != null ? ExtBidPrebidVideo.of(duration, null) : null)
                .meta(ExtBidPrebidMeta.builder()
                        .mediaType(inBannerVideo ? BidType.video.getName() : type.getName())
                        .build())
                .build();

        return extBid != null
                ? extBid.set(PREBID, mapper.mapper().valueToTree(extBidPrebid))
                : mapper.mapper().createObjectNode()
                .set(PREBID, mapper.mapper().valueToTree(extBidPrebid));
    }

    private static Integer getDuration(PubmaticBidExt bidExt) {
        return Optional.ofNullable(bidExt)
                .map(PubmaticBidExt::getVideo)
                .map(VideoCreativeInfo::getDuration)
                .orElse(null);
    }

    private static boolean getInBannerVideo(PubmaticBidExt bidExt) {
        return Optional.ofNullable(bidExt)
                .map(PubmaticBidExt::getInBannerVideo)
                .orElse(false);
    }

    private static Integer getDealPriority(PubmaticBidExt bidExt) {
        return Optional.ofNullable(bidExt)
                .map(PubmaticBidExt::getPrebidDealPriority)
                .orElse(null);
    }

    private static List<ExtIgi> extractIgi(PubmaticBidResponse bidResponse) {
        final List<ExtIgiIgs> igs = Optional.ofNullable(bidResponse)
                .map(PubmaticBidResponse::getExt)
                .map(PubmaticExtBidResponse::getFledgeAuctionConfigs)
                .orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(config -> ExtIgiIgs.builder().impId(config.getKey()).config(config.getValue()).build())
                .toList();

        return igs.isEmpty() ? null : Collections.singletonList(ExtIgi.builder().igs(igs).build());
    }
}
