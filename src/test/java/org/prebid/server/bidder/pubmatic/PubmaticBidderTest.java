package org.prebid.server.bidder.pubmatic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Audio;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Native;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import org.junit.jupiter.api.Test;
import org.prebid.server.VertxTest;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.CompositeBidderResponse;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.HttpResponse;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticBidderImpExt;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticExtDataAdServer;
import io.vertx.core.http.HttpMethod;
import org.prebid.server.bidder.pubmatic.model.request.PubmaticWrapper;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticBidExt;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticBidResponse;
import org.prebid.server.bidder.pubmatic.model.response.PubmaticExtBidResponse;
import org.prebid.server.bidder.pubmatic.model.response.VideoCreativeInfo;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidAlternateBidderCodes;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidAlternateBidderCodesBidder;
import org.prebid.server.proto.openrtb.ext.request.pubmatic.ExtImpPubmatic;
import org.prebid.server.proto.openrtb.ext.request.pubmatic.ExtImpPubmaticKeyVal;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidMeta;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidVideo;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponsePrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtIgi;
import org.prebid.server.proto.openrtb.ext.response.ExtIgiIgs;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.UnaryOperator.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.tuple;

public class PubmaticBidderTest extends VertxTest {

    private static final String ENDPOINT_URL = "http://test.endpoint.com/translator?source=prebid-server";
    private static final BidType AUDIO = BidType.audio;
    private static final BidType BANNER = BidType.banner;
    private static final BidType VIDEO = BidType.video;
    private static final BidType X_NATIVE = BidType.xNative;

    private final PubmaticBidder target = new PubmaticBidder(ENDPOINT_URL, jacksonMapper);

    private static ExtRequest expectedBidRequestExt(ExtRequest baseExt, int wrapperProfile, int wrapperVersion) {
        // Create wrapper node
        final ObjectNode wrapperNode = mapper.createObjectNode()
                .put("profile", wrapperProfile)
                .put("version", wrapperVersion);

        // Create bidder params with wrapper
        final ObjectNode bidderParams = mapper.createObjectNode()
                .set("wrapper", wrapperNode);

        // Create prebid with bidder params
        final ExtRequestPrebid prebid = ExtRequestPrebid.builder()
                .bidderparams(bidderParams)
                .build();

        // Create ext request with prebid and add wrapper to properties
        final ExtRequest extRequest = ExtRequest.of(prebid);
        extRequest.addProperties(Collections.singletonMap("wrapper", wrapperNode));
        return extRequest;
    }

    private static ObjectNode givenExtImpWithKadfloor(String kadfloor) {
        final ObjectNode extNode = mapper.createObjectNode();
        extNode.put("kadfloor", kadfloor);
        return extNode;
    }

    @Test
    public void creationShouldFailOnInvalidEndpointUrl() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PubmaticBidder("invalid_url", jacksonMapper));
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfImpExtCouldNotBeParsed() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder
                        .ext(mapper.valueToTree(ExtPrebid.of(null, mapper.createArrayNode()))));
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).hasSize(1);
        assertThat(result.getErrors().getFirst().getMessage()).startsWith("Cannot deserialize value");
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldNotUpdateBidfloorIfImpExtKadfloorIsInvalid() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder
                        .banner(Banner.builder().w(300).h(250).build())
                        .bidfloor(BigDecimal.TEN),
                extBuilder -> extBuilder.kadfloor("invalid"));
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor)
                .containsExactly(BigDecimal.TEN);
        assertThat(result.getErrors()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnBidfloorIfImpExtKadfloorIsValid() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(300).h(250).build()),
                extBuilder -> extBuilder.kadfloor("12.5"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor)
                .containsExactly(new BigDecimal("12.5"));
        assertThat(result.getErrors()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnBidfloorIfImpExtKadfloorIsValidAndResolvedWhitespace() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(300).h(250).build()),
                extBuilder -> extBuilder.kadfloor("  12.5  "));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor)
                .containsExactly(new BigDecimal("12.5"));
        assertThat(result.getErrors()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnMaxOfBidfloorAndKadfloorIfImpExtKadfloorIsValid() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder
                        .banner(Banner.builder().w(300).h(250).build())
                        .bidfloor(BigDecimal.ONE),
                extImpBuilder -> extImpBuilder.kadfloor("12.5")
        );

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor)
                .containsExactly(BigDecimal.valueOf(12.5));
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfBidRequestExtWrapperIsInvalid() {
        // given
        final ObjectNode pubmaticNode = mapper.createObjectNode();
        pubmaticNode.set("pubmatic", mapper.createObjectNode()
                .set("wrapper", mapper.createObjectNode()
                        .set("version", TextNode.valueOf("invalid"))));
        final ExtRequest bidRequestExt = ExtRequest.of(ExtRequestPrebid.builder().bidderparams(pubmaticNode).build());

        final BidRequest bidRequest = BidRequest.builder()
                .ext(bidRequestExt).build();
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue()).isEmpty();
        assertThat(result.getErrors()).hasSize(1)
                .allMatch(bidderError -> bidderError.getType().equals(BidderError.Type.bad_input)
                        && bidderError.getMessage().startsWith("Error converting bidder params in Pubmatic extension"));
    }

    @Test
    public void makeHttpRequestsShouldPreserveAlternateBidderCodesIfPresent() {
        // given
        final ExtRequestPrebidAlternateBidderCodes alternateBidderCodes = ExtRequestPrebidAlternateBidderCodes.of(
                true,
                Map.of("anotherbidder", ExtRequestPrebidAlternateBidderCodesBidder.of(true,
                                                                                        java.util.Set.of("pubmatic"))));
        final BidRequest bidRequest = givenBidRequest(identity())
                .toBuilder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder()
                                .w(300)
                                .h(250)
                        .build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                ExtImpPubmatic.builder()
                                        .publisherId("pubId")
                                        .wrapper(PubmaticWrapper.of(123, 999))
                                        .build())))
                .build()))
                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .alternateBidderCodes(alternateBidderCodes)
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .extracting(ext -> ext.getPrebid().getAlternateBidderCodes())
                .containsExactly(alternateBidderCodes);
    }

    @Test
    public void makeHttpRequestsShouldNotPreserveAlternateBidderCodesIfEmpty() {
        // given
        final ExtRequestPrebidAlternateBidderCodes alternateBidderCodes = ExtRequestPrebidAlternateBidderCodes.of(
                true,
                Collections.emptyMap());

        final BidRequest bidRequest = givenBidRequest(identity())
                .toBuilder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder()
                                .w(300)
                                .h(250)
                        .build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                ExtImpPubmatic.builder()
                                        .publisherId("pubId")
                                        .wrapper(PubmaticWrapper.of(123, 999))
                                        .build())))
                .build()))
                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .alternateBidderCodes(alternateBidderCodes)
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .allSatisfy(ext -> assertThat(ext.getPrebid().getAlternateBidderCodes()).isNull());
    }

    @Test
    public void makeHttpRequestsShouldReturnBidRequestExtIfAcatFieldIsValidAndTrimWhitespace() {
        // given
        final ObjectNode pubmaticNode = mapper.createObjectNode();
        pubmaticNode.set("pubmatic", mapper.createObjectNode()
                .set("acat", mapper.createArrayNode()
                        .add("\tte st Value\t").add("test Value").add("Value")));

        final ExtRequest bidRequestExt = ExtRequest.of(ExtRequestPrebid.builder().bidderparams(pubmaticNode).build());
        final BidRequest bidRequest = BidRequest.builder()
                .ext(bidRequestExt)
                .imp(singletonList(Imp.builder()
                    .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                            ExtImpPubmatic.builder().wrapper(PubmaticWrapper.of(1, 1)).build())))
                    .build()))
                .build();
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        final ObjectNode bidderParams = mapper.createObjectNode();
        bidderParams.set("wrapper", mapper.createObjectNode()
                .put("profile", 1)
                .put("version", 1));
        bidderParams.set("acat", mapper.createArrayNode()
                .add("te st Value")
                .add("test Value")
                .add("Value"));

        final ExtRequest expectedExtRequest = ExtRequest.of(ExtRequestPrebid.builder()
                .bidderparams(bidderParams)
                .build());
        expectedExtRequest.addProperty("acat",
                mapper.createArrayNode().add("te st Value").add("test Value").add("Value"));
        expectedExtRequest.addProperty("wrapper",
                mapper.createObjectNode().put("profile", 1).put("version", 1));

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .containsExactly(expectedExtRequest);
    }

    @Test
    public void makeHttpRequestsShouldMergeWrappersFromImpAndBidRequestExt() {
        // given
        final ObjectNode pubmaticNode = mapper.createObjectNode();
        pubmaticNode.set("pubmatic", mapper.createObjectNode()
                .set("wrapper", mapper.valueToTree(PubmaticWrapper.of(123, 456))));

        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(
                        Imp.builder()
                                .id("123")
                                .banner(Banner.builder().w(300).h(250).build())
                                .ext(mapper.valueToTree(ExtPrebid.of(null,
                                        ExtImpPubmatic.builder()
                                                .publisherId("pubId")
                                                .wrapper(PubmaticWrapper.of(123, 999))
                                                .build())))
                                .build()))
                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .bidderparams(pubmaticNode)
                        .build()))
                .build();
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .containsExactly(expectedBidRequestExt(bidRequest.getExt(), 123, 456));
    }

    @Test
    public void makeHttpRequestsShouldNotReturnErrorIfNativePresent() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(300).h(250).build())
                        .xNative(Native.builder().build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1);
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfNoBannerOrVideoOrNative() {
        // given
        final BidRequest bidRequest = givenBidRequest(identity());

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors())
                .containsExactly(BidderError.badInput(
                        "Invalid MediaType. PubMatic only supports Banner, Video and Native. Ignoring ImpID=123"));
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldUseWrapperFromBidRequestExtIfPresent() {
        // given
        final ObjectNode pubmaticNode = mapper.createObjectNode()
                .set("wrapper", mapper.valueToTree(PubmaticWrapper.of(21, 33)));

        final ExtRequest bidRequestExt = ExtRequest.of(ExtRequestPrebid.builder()
                .bidderparams(pubmaticNode)
                .build());
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder
                        .id("123")
                        .banner(Banner.builder().w(300).h(250).build()),
                extBuilder -> extBuilder
                        .wrapper(PubmaticWrapper.of(1, 1))
        ).toBuilder()
                .ext(bidRequestExt)
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .extracting(ext -> ext.getProperties().get("wrapper"))
                .isNotNull();

    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfAdSlotIsInvalid() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.adSlot("invalid ad slot@"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors())
                .containsExactly(BidderError.badInput("Invalid adSlot 'invalid ad slot@'"));
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfAdSlotHasInvalidSizes() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.adSlot("slot@300x200x100"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors())
                .containsExactly(BidderError.badInput("Invalid size provided in adSlot 'slot@300x200x100'"));
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfAdSlotHasInvalidWidth() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.adSlot("slot@widthx200"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors())
                .containsExactly(BidderError.badInput("Invalid width provided in adSlot 'slot@widthx200'"));
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorIfAdSlotHasInvalidHeight() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.adSlot("slot@300xHeight:1"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors())
                .containsExactly(BidderError.badInput("Invalid height provided in adSlot 'slot@300xHeight:1'"));
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    public void makeHttpRequestsShouldSetAudioToNullIfPresent() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.audio(Audio.builder().build()).banner(Banner.builder().build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getAudio)
                .containsNull();
    }

    @Test
    public void makeHttpRequestsShouldSetBannerWidthAndHeightFromAdSlot() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBanner)
                .extracting(Banner::getH)
                .containsExactly(250);
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBanner)
                .extracting(Banner::getW)
                .containsExactly(300);
    }

    @Test
    public void makeHttpRequestsShouldSetBannerWidthAndHeightFromFormatIfMissedOriginalsOrInAdSlot() {
        // given
        final BidRequest bidRequest = givenBidRequest(impBuilder -> impBuilder.banner(Banner.builder()
                .format(singletonList(Format.builder().w(100).h(200).build()))
                .build()), extImpPubmaticBuilder -> extImpPubmaticBuilder.adSlot(null));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBanner)
                .extracting(Banner::getH)
                .containsExactly(200);
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBanner)
                .extracting(Banner::getW)
                .containsExactly(100);
    }

    @Test
    public void makeHttpRequestsShouldSetTagIdForBannerImpsWithSymbolsFromAdSlotBeforeAtSign() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getTagid)
                .containsExactly("slot");
    }

    @Test
    public void makeHttpRequestsShouldSetTagIdForVideoImpsWithSymbolsFromAdSlotBeforeAtSign() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder
                        .video(Video.builder().w(300).h(250).build())
                        .banner(Banner.builder().build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getTagid)
                .containsExactly("slot");
    }

    @Test
    public void makeHttpRequestsShouldSetAdSlotAsTagIdIfAtSignIsMissing() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().build()),
                extImpBuilder -> extImpBuilder
                        .adSlot("adSlot"));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getTagid)
                .containsExactly("adSlot");
    }

    @Test
    public void makeHttpRequestsShouldSetImpExtNullIfKeywordsAreNull() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsNull();
    }

    @Test
    public void makeHttpRequestsShouldSetImpExtNullIfKeywordsAreEmpty() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.keywords(emptyList()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsNull();
    }

    public void makeHttpRequestsShouldSetImpExtFromKeywords() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                identity(),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.keywords(singletonList(
                                ExtImpPubmaticKeyVal.of("key2", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("key2", "value1,value2");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtAddUnitKeyKeyWordFromDataAdSlotIfAdServerNameIsNotGam() {
        // given
        final ObjectNode extData = mapper.createObjectNode()
                .put("pbadslot", "pbaAdSlot")
                .set("adserver", mapper.valueToTree(new PubmaticExtDataAdServer("adServerName", "adServerAdSlot")));
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().build(),
                                extData,
                                null,
                                null
                        )))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("dfp_ad_unit_code", "pbaAdSlot");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtAddUnitKeyKeyWordFromAdServerAdSlotIfAdServerNameIsGam() {
        // given
        final ObjectNode extData = mapper.createObjectNode()
                .put("pbadslot", "pbaAdSlot")
                .set("adserver", mapper.valueToTree(new PubmaticExtDataAdServer("gam", "adServerAdSlot")));
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().build(),
                                extData,
                                null,
                                null
                        )))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("dfp_ad_unit_code", "adServerAdSlot");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtWithKeyValWithDctrAndExtDataExceptForPbaSlotAndAdServer() {
        // given
        final ObjectNode extData = mapper.createObjectNode()
                .put("pbadslot", "pbaAdSlot")
                .put("key1", "value")
                .set("adserver", mapper.valueToTree(new PubmaticExtDataAdServer("gam", "adServerAdSlot")));
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().dctr("dctr").build(),
                                extData,
                                null,
                                null
                        )))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("dfp_ad_unit_code", "adServerAdSlot");
        expectedImpExt.put("key_val", "dctr|key1=value");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtWithKeyValWithExtDataWhenDctrIsAbsent() {
        // given
        final ObjectNode extData = mapper.createObjectNode()
                .put("key1", "  value")
                .put("key2", 1)
                .put("key3", true)
                .put("key4", 3.42)
                .set("key5", mapper.createArrayNode().add("elem1").add("elem2"));
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().dctr(null).build(),
                                extData,
                                null,
                                null
                        )))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("key_val", "key1=value|key2=1|key3=true|key4=3.42|key5=elem1,elem2");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtAddAE() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().build(), null, 1, null)))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode().put("ae", 1);
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldAddImpExtAddGpId() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(PubmaticBidderImpExt.of(
                                ExtImpPubmatic.builder().build(), null, null, "gpId")))
                        .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode().put("gpid", "gpId");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldSetImpExtFromKeywordsSkippingKeysWithEmptyValues() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder
                        .keywords(asList(
                                ExtImpPubmaticKeyVal.of("key with empty value", emptyList()),
                                ExtImpPubmaticKeyVal.of("key2", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final ObjectNode expectedImpExt = mapper.createObjectNode();
        expectedImpExt.put("key2", "value1,value2");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(expectedImpExt);
    }

    @Test
    public void makeHttpRequestsShouldSetRequestExtFromWrapExt() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder
                        .wrapper(PubmaticWrapper.of(1, 1)));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .extracting(ext -> ext.getProperties().get("wrapper"))
                .isNotNull();
    }

    @Test
    public void makeHttpRequestsShouldNotChangeExtIfWrapExtIsMissing() {
        // given
        final BidRequest bidRequest = givenBidRequest(identity())
                .toBuilder()
                .imp(singletonList(Imp.builder()
                        .banner(Banner.builder().w(1).h(1).build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null, ExtImpPubmatic.builder().build())))
                        .build()))
                .ext(ExtRequest.empty())  // This creates ExtRequest with prebid = null
                .build();
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getExt)
                .containsExactly(ExtRequest.empty());
    }

    @Test
    public void makeHttpRequestsShouldSetSitePublisherIdFromImpExtPublisherId() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .imp(Collections.singletonList(Imp.builder()
                        .banner(Banner.builder().w(300).h(250).build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                                ExtImpPubmatic.builder().publisherId("imp pub id").build())))
                        .build()))
                .site(Site.builder().build())
                .build();
        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getSite)
                .extracting(Site::getPublisher)
                .extracting(Publisher::getId)
                .containsExactly("imp pub id");
    }

    @Test
    public void makeHttpRequestsShouldUpdateSitePublisherIdFromImpExtPublisherId() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .imp(Collections.singletonList(Imp.builder()
                        .banner(Banner.builder().w(300).h(250).build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null, ExtImpPubmatic.builder()
                                .publisherId("pub id imp")
                                .wrapper(PubmaticWrapper.of(1, 1))
                                .build())))
                        .build()))
                .site(Site.builder()
                        .publisher(Publisher.builder()
                                .id("pub id")
                                .build())
                        .build())
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getSite)
                .extracting(Site::getPublisher)
                .extracting(Publisher::getId)
                .containsExactly("pub id imp");
    }

    @Test
    public void makeHttpRequestsShouldSetAppPublisherIdIfSiteIsNull() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .app(App.builder()
                        .publisher(Publisher.builder().id("anotherId").build())
                        .build())
                .imp(Collections.singletonList(
                        Imp.builder()
                                .id("123")
                                .banner(Banner.builder().w(300).h(250).build())
                                .ext(mapper.valueToTree(
                                        ExtPrebid.of(null,
                                                ExtImpPubmatic.builder()
                                                        .publisherId("pub id")
                                                        .build())))
                                .build()))
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue())
                .extracting(HttpRequest::getPayload)
                .extracting(BidRequest::getApp)
                .extracting(App::getPublisher)
                .extracting(Publisher::getId)
                .containsExactly("pub id");
    }

    @Test
    public void makeHttpRequestsShouldFillMethodAndUrlAndExpectedHeaders() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue()).doesNotContainNull()
                .hasSize(1).element(0)
                .returns(HttpMethod.POST, HttpRequest::getMethod)
                .returns("http://test.endpoint.com/translator?source=prebid-server", HttpRequest::getUri);
        assertThat(result.getValue().getFirst().getHeaders()).isNotNull()
                .extracting(Map.Entry::getKey, Map.Entry::getValue)
                .containsOnly(
                        tuple(HttpUtil.CONTENT_TYPE_HEADER.toString(), "application/json;charset=utf-8"),
                        tuple(HttpUtil.ACCEPT_HEADER.toString(), "application/json"));
    }

    @Test
    public void makeHttpRequestsShouldReplaceDctrIfPresent() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.dctr("dctr")
                        .keywords(singletonList(ExtImpPubmaticKeyVal.of("key_val", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final Map<String, String> expectedKeyWords = singletonMap("key_val", "dctr");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(mapper.convertValue(expectedKeyWords, ObjectNode.class));
    }

    @Test
    public void makeHttpRequestsShouldReplacePmZoneIdIfPresent() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder.pmZoneId("pmzoneid")
                        .keywords(singletonList(ExtImpPubmaticKeyVal.of("pmZoneId", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final Map<String, String> expectedKeyWords = singletonMap("pmZoneId", "pmzoneid");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(mapper.convertValue(expectedKeyWords, ObjectNode.class));
    }

    @Test
    public void makeHttpRequestsShouldReplacePmZoneIDOldKeyNameWithNew() {
        // given
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extImpPubmaticBuilder -> extImpPubmaticBuilder
                        .keywords(singletonList(ExtImpPubmaticKeyVal.of("pmZoneID", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final Map<String, String> expectedKeyWords = singletonMap("pmZoneId", "value1,value2");
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .containsExactly(mapper.convertValue(expectedKeyWords, ObjectNode.class));
    }

    @Test
    public void makeHttpRequestsShouldConvertKeywordCaseCorrectly() {
        final BidRequest bidRequest = givenBidRequest(
                impBuilder -> impBuilder.banner(Banner.builder().w(1).h(1).build()),
                extBuilder -> extBuilder.wrapper(PubmaticWrapper.of(321, 456))
                        .keywords(singletonList(
                                ExtImpPubmaticKeyVal.of("pmZoneID", asList("value1", "value2")))));

        // when
        final Result<List<HttpRequest<BidRequest>>> result = target.makeHttpRequests(bidRequest);

        // then
        final Map<String, String> expectedKeyWords = singletonMap("pmZoneId", "value1,value2");
        final List<BidderError> bidderErrors = result.getErrors();
        assertThat(bidderErrors).isEmpty();
        final List<HttpRequest<BidRequest>> httpRequests = result.getValue();
        assertThat(httpRequests).hasSize(1);

        final BidRequest modifiedRequest = httpRequests.get(0).getPayload();
        final List<Imp> imps = modifiedRequest.getImp();
        assertThat(imps).hasSize(1);

        final ObjectNode impExt = imps.get(0).getExt();
        assertThat(impExt).isEqualTo(mapper.convertValue(expectedKeyWords, ObjectNode.class));
    }

    @Test
    public void makeBidderResponseShouldReturnErrorIfResponseBodyCouldNotBeParsed() {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall("invalid");

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).hasSize(1);
        assertThat(result.getErrors().getFirst().getMessage()).startsWith("Failed to decode: Unrecognized token");
        assertThat(result.getErrors().getFirst().getType()).isEqualTo(BidderError.Type.bad_server_response);
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidderResponseShouldReturnEmptyListIfBidResponseIsNull() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(null));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidderResponseShouldReturnEmptyListIfBidResponseSeatBidIsNull() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(BidResponse.builder().build()));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidderResponseShouldReturnBannerBid() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(1))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "banner")));

        assertThat(result.getBids()).containsExactly(BidderBid.of(
                Bid.builder().impid("123").mtype(1).ext(expectedBidExt).build(), BANNER, "USD"));
    }

    @Test
    public void makeBidderResponseShouldReturnVideoBidWhenInBannerVideoIsTrue() throws JsonProcessingException {
        // given
        final ObjectNode givenBidExt = mapper.createObjectNode().put("ibv", true);
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(givenBidResponse(
                        bidBuilder -> bidBuilder.impid("123").mtype(1).ext(givenBidExt))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode expectedBidExt = mapper.createObjectNode()
                .put("ibv", true)
                .set("prebid", mapper.createObjectNode()
                        .set("meta", mapper.createObjectNode().put("mediaType", "video")));

        assertThat(result.getBids()).containsExactly(BidderBid.of(
                Bid.builder().impid("123").mtype(1).ext(expectedBidExt).build(), BANNER, "USD"));
    }

    @Test
    public void makeBidderResponseShouldReturnVideoBidIfExtBidContainsMtypeTwo() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(2))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "video")));

        assertThat(result.getBids()).containsExactly(
                BidderBid.of(Bid.builder().impid("123").mtype(2).ext(expectedBidExt).build(), VIDEO, "USD"));
    }

    @Test
    public void makeBidderResponseShouldReturnAudioBidIfExtBidContainsMtype3() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(3))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "audio")));

        assertThat(result.getBids()).containsExactly(
                BidderBid.of(Bid.builder().impid("123").mtype(3).ext(expectedBidExt).build(), AUDIO, "USD"));
    }

    @Test
    public void makeBidderResponseShouldReturnXNativeBidIfExtBidContainsMtypeFour() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(4))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "native")));

        assertThat(result.getBids()).containsExactly(
                BidderBid.of(Bid.builder().impid("123").mtype(4).ext(expectedBidExt).build(), X_NATIVE, "USD"));
    }

    @Test
    public void makeBidderResponseShouldFillExtBidPrebidVideoDurationIfDurationIsNotNull()
            throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(2)
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(VideoCreativeInfo.of(1), null, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();

        final ObjectNode bidExt = mapper.createObjectNode();
        bidExt.set("video", mapper.valueToTree(VideoCreativeInfo.of(1)));
        bidExt.set("prebid", mapper.valueToTree(ExtBidPrebid.builder()
                .meta(ExtBidPrebidMeta.builder().mediaType("video").build())
                .video(ExtBidPrebidVideo.of(1, null))
                .build()));
        assertThat(result.getBids()).containsExactly(
                BidderBid.of(Bid.builder().mtype(2).impid("123").ext(bidExt).build(), VIDEO, "USD"));
    }

    @Test
    public void makeBidderResponseShouldNotFillExtBidPrebidVideoDurationIfDurationIsNull()
            throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(VideoCreativeInfo.of(null), null, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        final ObjectNode bidExt = mapper.createObjectNode();
        bidExt.set("video", mapper.valueToTree(VideoCreativeInfo.of(null)));
        bidExt.set("prebid", mapper.valueToTree(ExtBidPrebid.builder()
                .meta(ExtBidPrebidMeta.builder().mediaType("banner").build())
                .build()));

        assertThat(result.getBids()).containsExactly(
                BidderBid.of(Bid.builder().impid("123").mtype(1).ext(bidExt).build(), BANNER, "USD"));
    }

    @Test
    public void makeBidderResponseShouldNotFillExtBidPrebidVideoDurationIfVideoIsNull() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(null, null, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "banner")));

        assertThat(result.getBids()).containsExactly(BidderBid.of(
                Bid.builder().impid("123").mtype(1).ext(expectedBidExt).build(), BANNER, "USD"));
    }

    @Test
    public void makeBidderResponseShouldFillDealPriorityData() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(null, 12, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids())
                .extracting(BidderBid::getDealPriority)
                .containsExactly(12);
    }

    @Test
    public void makeBidderResponseShouldFillSeat() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(null, 12, "marketplace", null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids())
                .extracting(BidderBid::getSeat)
                .containsExactly("marketplace");
    }

    @Test
    public void makeBidderResponseShouldParseNativeAdmData() throws JsonProcessingException {
        // given
        final ObjectNode admNode = mapper.createObjectNode();
        final ObjectNode nativeNode = mapper.createObjectNode();
        nativeNode.put("property1", "value1");
        admNode.set("native", nativeNode);
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(4)
                                .adm(admNode.toString())
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(null, 12, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getAdm)
                .containsExactly("{\"property1\":\"value1\"}");
    }

    @Test
    public void makeBidderResponseShouldTakeOnlyFirstCatElement() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .cat(asList("cat1", "cat2")))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "banner")));

        assertThat(result.getBids()).containsExactly(BidderBid.of(
                Bid.builder()
                        .impid("123")
                        .mtype(1)
                        .cat(singletonList("cat1"))
                        .ext(expectedBidExt).build(),
                BANNER,
                "USD"));
    }

    @Test
    public void makeBidderResponseShouldReturnBannerBidIfExtBidContainsIllegalBidType() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(givenBidResponse(bidBuilder -> bidBuilder.impid("123").mtype(100))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).containsExactly(
                BidderError.badServerResponse("failed to parse bid mtype (100) for impression id 123"));
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidderResponseShouldReturnFledgeAuctionConfig() throws JsonProcessingException {
        // given
        final BidResponse bidResponse = givenBidResponse(bidBuilder -> bidBuilder.impid("imp_id"));
        final ObjectNode auctionConfig = mapper.createObjectNode();
        final PubmaticBidResponse bidResponseWithFledge = PubmaticBidResponse.builder()
                .cur(bidResponse.getCur())
                .seatbid(bidResponse.getSeatbid())
                .ext(PubmaticExtBidResponse.of(Map.of("imp_id", auctionConfig)))
                .build();
        final BidderCall<BidRequest> httpCall = givenHttpCall(mapper.writeValueAsString(bidResponseWithFledge));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        final ObjectNode expectedBidExt = mapper.createObjectNode().set("prebid", mapper.createObjectNode()
                .set("meta", mapper.createObjectNode().put("mediaType", "banner")));

        assertThat(result.getBids()).containsExactly(BidderBid.of(
                Bid.builder().impid("imp_id").mtype(1).ext(expectedBidExt).build(), BANNER, "USD"));

        final ExtIgi igi = ExtIgi.builder()
                .igs(singletonList(ExtIgiIgs.builder().impId("imp_id").config(auctionConfig).build()))
                .build();

        assertThat(result.getIgi()).containsExactly(igi);
    }

    @Test
    public void makeBidderResponseShouldNotModifyAdmWhenNativeNodeIsNull() throws JsonProcessingException {
        // given
        final ObjectNode admNode = mapper.createObjectNode().put("otherField", "value");
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123")
                                .adm(admNode.toString())
                                .ext(mapper.valueToTree(
                                        PubmaticBidExt.of(null, 12, null, null))))));

        // when
        final CompositeBidderResponse result = target.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getAdm)
                .containsExactly(admNode.toString());
    }

    @Test
    public void makeBidsShouldFail() throws JsonProcessingException {
        //given
        final BidderCall<BidRequest> httpCall = givenHttpCall(mapper.writeValueAsString(BidResponse.builder().build()));

        // when
        final Result<List<BidderBid>> result = target.makeBids(httpCall, null);

        // then
        assertThat(result.getErrors()).containsExactly(BidderError.generic("Deprecated adapter method invoked"));
    }

    private static BidRequest givenBidRequest(UnaryOperator<Imp.ImpBuilder> impCustomizer) {
        return givenBidRequest(impCustomizer, extImpPubmaticBuilder -> extImpPubmaticBuilder);
    }

    private static BidRequest givenBidRequest(UnaryOperator<Imp.ImpBuilder> impCustomizer,
                                             UnaryOperator<ExtImpPubmatic.ExtImpPubmaticBuilder> extCustomizer) {

        final Imp imp = impCustomizer.apply(Imp.builder()
                        .id("123")
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                extCustomizer.apply(ExtImpPubmatic.builder()
                                                .publisherId("pub id")
                                                .adSlot("slot@300x250"))
                        .build()))))
                .build();

        return BidRequest.builder()
                .id("requestId")
                .imp(singletonList(imp))
                .build();
    }

    private static BidRequest givenBidRequest(UnaryOperator<Imp.ImpBuilder> impCustomizer,
                                             UnaryOperator<ExtImpPubmatic.ExtImpPubmaticBuilder> extCustomizer,
                                             PubmaticWrapper wrapper) {

        final ExtImpPubmatic extImpPubmatic = extCustomizer
                .apply(ExtImpPubmatic.builder().wrapper(wrapper))
                .build();

        final PubmaticBidderImpExt bidderImpExt = createBidderImpExt(extImpPubmatic);

        final Imp imp = impCustomizer
                .apply(Imp.builder().ext(jacksonMapper.mapper().valueToTree(bidderImpExt)))
                .build();

        return BidRequest.builder()
                .id("requestId")
                .imp(singletonList(imp))
                .build();
    }

    private static BidResponse givenBidResponse(UnaryOperator<Bid.BidBuilder> bidCustomizer) {
        return BidResponse.builder()
                .cur("USD")
                .seatbid(singletonList(SeatBid.builder()
                        .bid(singletonList(bidCustomizer.apply(Bid.builder().mtype(1)).build()))
                        .build()))
                .ext(ExtBidResponse.builder().prebid(ExtBidResponsePrebid.builder().build()).build())
                .build();
    }

    private static BidderCall<BidRequest> givenHttpCall(String body) {
        return BidderCall.succeededHttp(
                HttpRequest.<BidRequest>builder().payload(null).build(),
                HttpResponse.of(200, null, body),
                null);
    }

    private static ObjectNode createWrapperParams(String profile, String version) {
        final ObjectNode wrapperNode = mapper.createObjectNode()
                .put("profile", profile)
                .put("version", version);
        return mapper.createObjectNode().set("wrapper", wrapperNode);
    }

    private static PubmaticBidderImpExt createBidderImpExt(ExtImpPubmatic extImpPubmatic) {
        return PubmaticBidderImpExt.of(
                extImpPubmatic,
                null,
                null,
                null
        );
    }
}
