from io import StringIO
from unittest.mock import MagicMock

from bs4 import BeautifulSoup
from pytest import fixture, raises

from cif.extractor import (
    AggregationLevel,
    CSVRowExtractor,
    Extractor,
    HTMLExtractor,
    HTMLLinkExtractor,
    HTMLTitleExtractor,
    TextContentFilterModel,
)
from cif.persistence import ArtifactModel, FragmentKeyModel, FragmentModel
from cif.util import now


@fixture
def artifact() -> ArtifactModel:
    return ArtifactModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        version="version",
        external_id="path/to/artifact/logical_id.html",
        created_on=now(),
        content_length=0,
        content_type="text/html",
    )


@fixture
def fragment() -> FragmentModel:
    return FragmentModel(
        source_id="source_id",
        artifact_id="artifact_id",
        fragment_id="fragment_id",
        seq_no=0,
        aggregation_level=AggregationLevel.DOCUMENT,
        text_content="text_content",
    )


def test_extractor_calc_fragments_not_implemented(artifact: ArtifactModel) -> None:
    with raises(NotImplementedError, match="calc_fragments"):
        Extractor().calc_fragments(artifact)


def test_html_extractor_parse_content(artifact: ArtifactModel) -> None:
    content = b"<!DOCTYPE html>\n\n<html><head><title>title</title></head><body></body></html>"
    connector = MagicMock()
    connector.get_artifact.return_value = iter([content]), None
    extractor = HTMLExtractor(connector)
    soup = extractor.parse_content(artifact.external_id)
    assert soup.decode_contents() == content.decode("utf-8")
    connector.get_artifact.assert_called_once_with(artifact.external_id)


def test_html_extractor_calc_fragments(artifact: ArtifactModel) -> None:
    extractor = HTMLExtractor.new_instance(MagicMock())
    extractor.parse_content = MagicMock()
    extractor.parse_content.return_value = BeautifulSoup(
        b"<!DOCTYPE html>\n\n<html><head><title>title</title></head><body><p>"
        + b"This     has &nbsp; extra\n\n\n whitespace</p></body></html>",
        "html.parser",
    )
    fragments = extractor.calc_fragments(artifact)
    assert len(fragments) == 1
    assert fragments[0].text_content == "title This has extra whitespace"
    assert fragments[0].aggregation_level == AggregationLevel.DOCUMENT
    extractor.parse_content.assert_called_once_with(artifact.external_id)


def test_link_extractor_calc_fragments(artifact: ArtifactModel) -> None:
    extractor = HTMLLinkExtractor.new_instance(MagicMock())
    extractor.parse_content = MagicMock()
    extractor.parse_content.return_value = BeautifulSoup(
        b'<!DOCTYPE html>\n\n<html><head><title>title</title></head><body><a href="href">link_content</a></body></html>',
        "html.parser",
    )
    fragments = extractor.calc_fragments(artifact)
    assert len(fragments) == 1
    assert fragments[0].text_content == "href link_content"
    assert fragments[0].aggregation_level == AggregationLevel.LINK
    extractor.parse_content.assert_called_once_with(artifact.external_id)


def test_title_extractor_calc_fragments(artifact: ArtifactModel) -> None:
    extractor = HTMLTitleExtractor.new_instance(MagicMock())
    extractor.parse_content = MagicMock()
    extractor.parse_content.return_value = BeautifulSoup(
        b"""<!DOCTYPE html>\n\n<html><head><title>D0003 - Diagnostic High Level Claim Handling</title>
        </head><body><a href="href">link_content</a></body></html>""",
        "html.parser",
    )
    fragments = extractor.calc_fragments(artifact)
    assert len(fragments) == 1
    assert fragments[0].text_content == "D0003 - Diagnostic High Level Claim Handling"
    assert fragments[0].aggregation_level == AggregationLevel.TITLE
    extractor.parse_content.assert_called_once_with(artifact.external_id)


def test_csv_row_extractor_parse_content(artifact: ArtifactModel) -> None:
    content = b"foo,bar,baz\n0,1,quux\n"
    connector = MagicMock()
    connector.get_artifact.return_value = iter([content]), None
    extractor = CSVRowExtractor(connector)
    parsed_content = extractor.parse_content(artifact.external_id)
    assert parsed_content.read() == StringIO(content.decode("utf-8")).read()
    connector.get_artifact.assert_called_once_with(artifact.external_id)
    connector.get_artifact_chunk.assert_not_called()


def test_csv_row_extractor_parse_content_range(artifact: ArtifactModel) -> None:
    content = b"foo,bar,baz\n0,1,quux\n"
    connector = MagicMock()
    connector.get_artifact_chunk.return_value = content
    extractor = CSVRowExtractor(connector)
    parsed_content = extractor.parse_content(artifact.external_id, start_byte=1000, end_byte=2000)
    assert parsed_content.read() == StringIO(content.decode("utf-8")).read()
    connector.get_artifact_chunk.assert_called_once_with(artifact.external_id, 1000, 2000)
    connector.get_artifact.assert_not_called()


def test_csv_header_row_calc_fieldnames(artifact: ArtifactModel) -> None:
    content = b"foo,bar,baz\n0,1,quux\n"
    connector = MagicMock()
    connector.get_artifact_chunk.return_value = content
    extractor = CSVRowExtractor(connector)
    fieldnames = extractor.calc_fieldnames(artifact)
    assert fieldnames == ["foo", "bar", "baz"]


def test_csv_row_extractor_calc_fragments(artifact: ArtifactModel) -> None:
    extractor = CSVRowExtractor.new_instance(MagicMock())
    extractor.parse_content = MagicMock()
    extractor.parse_content.return_value = StringIO(b"0,1,quux\n".decode("utf-8"))
    extractor.calc_fieldnames = MagicMock()
    extractor.calc_fieldnames.return_value = ["foo", "bar", "baz"]
    fragments = extractor.calc_fragments(artifact)
    assert len(fragments) == 1
    assert fragments[0].text_content == "0 1 quux"
    assert fragments[0].json_content == {"foo": "0", "bar": "1", "baz": "quux"}
    extractor.parse_content.assert_called_once_with(artifact.external_id, start_byte=None, end_byte=None)


def test_extractor_calc_fragment_keys_8eb156a290f14963a36a86ec6c5259d0(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "8eb156a290f14963a36a86ec6c5259d0"
    artifact.external_id = "epolicies_20250407/content/Dental/Restorative_Codes/D2710_D2792.html"
    fragment.text_content = """D2710 D2794 Crowns e Policies Policies Topics e Notes Tools Training D2710 D2794 Crowns n Crowns D2710 D2720 D2721 D2722 D2740 D2750 D2751 D2752 D2753 D2780 D2781 D2782 D2783 D2790 D2791 D2792 n Definition n General Claim Handling n Frequency Limit n Claim Processing Revision Date 12 10 2024 07 04 2023 05 16 2023 10 07 2019 08 06 2019 09 20 2016 11 03 2015 05 12 2015 01 01 2013 Definition Classifications Metals The noble metal classification system adopted precise method reporting various alloys used dentistry The alloys defined basis percentage metal content CLASSIFICATION REQUIREMENT High Noble Alloys Noble Metal Content 60 gold platinum group gold 40 Titanium Titanium Alloys Titanium 85 Noble Alloys Noble Metal Content 25 gold platinum group Predominantly Base Alloys Noble Metal Content 25 gold platinum group metals platinum group platinum palladium rhodium iridium osmium ruthenium Porcelain ceramic crowns presently include ceramic porcelain porcelain fused metal crowns Resin crowns resin metal crowns include reinforced heat pressure-cured resin materials CAD CAM see code D2740 General Claim Handling Please Note Permanent Crowns subject frequency temporary crown previously placed Code Description D2710 Crown resin-based composite indirect Unfilled non-reinforced resin crowns reported using D2999 D2720 Crown resin high noble metal D2721 Crown resin predominantly base metal D2722 Crown resin noble metal D2740 Crown porcelain ceramic D2750 Crown porcelain fused high noble metal D2751 Crown porcelain fused predominantly base metal D2752 Crown porcelain fused noble metal D2753 Crown porcelain fused titanium titanium alloys Effective 01 01 2020 D2780 Crown 3 4 cast high noble metal D2781 Crown 3 4 cast predominately base metal D2782 Crown 3 4 cast noble metal D2783 Crown 3 4 porcelain ceramic D2790 Crown full cast high noble metal D2791 Crown full cast predominantly base metal D2792 Crown full cast noble metal D2794 Crown Titanium titanium alloys Note If participating dentist bills upgrade special crown materials ex Zirconia Captek Lava considered enhanced techniques The participating dentist permitted bill members upgrades types crowns even member agree upgrade fee The dentist permitted charge negotiated fee appropriate ADA crown procedure code Submissions Primary Teeth ACAS The ACAS system deny service situations primary tooth submitted one crown procedure codes The denial appear 016 This tooth conflict edit Please follow steps situations Step Action 1 Verify 016 edit appears primary tooth 2 Manually refer claim UM This requires manually creating Cover Sheet CS Follow standard rules UM referral The necessary diagnostics x-rays narrative explaining rationale rendering service primary teeth Continue step 3 claim returned UM completed review 3 Perform A line level override Perform Q line level override Result The 016 edit removed 4 IF UM denied service THEN deny using appropriate action code corresponding UM response code IF UM allows service THEN consider service allowable The handling products document applies standard plans Please refer CCI specific handling Frequency Limit Product Frequency Edits Against Original Aetna PPO Indemnity NA NA Original PHC Plans 1 per tooth every 5 rolling day years Edits cast restorations tooth 2510-2794 2961-2962 New Dental 2000 Plans 1 per tooth every 5 8 rolling day years Edits cast restorations tooth 2510 2794 2961-2962 IFP Adult Dental 1 every 8 years Edits cast restorations tooth 2510 2794 2961 2962 Claim Processing Products Action Code Message Code All Products Except IFP Adult Dental Eligible standard plans Follow UM editing If denied based frequency limits NOT refer UM 5 yrs A46 8 yrs A47 026 Original PHC New Dental 2000 Plans provide alternate benefit D2792 A19 A20 When two permanent crown inlay onlay codes submitted tooth dates service within 120 days system deny second crown previously considered When two permanent crown inlay onlay codes submitted tooth dates service within 120 days system deny second crown previously considered NOTE If provider questions denial 2nd crown inlay onlay Deny frequency plans frequency Refer UM plans without frequency 770 The system deny crowns replace abutment crowns tooth within 60 months These reconsidered extraction would allow bridge placed Refer DR 02-01 026 IFP Adult Dental Eligible polan Follow UM editing If denied based frequency limits NOT refer UM AA47 Apply cosmetic exclusion performed th 1-3 14-16 17-19 30-32 Refer cosmetic table IFP 24-01 Dental IFP Plan Information TBD Manually refer UM review submission primary tooth Note DMO Crowns usually considered encounter services performed Primary Care Dentist Some Primary Care Dentists may special deal arrangements pay benefits per claim basis case standard claim handling followed Note Fixed Copay Plans No Additional Information Modal title Close Choose Category Select Service Category Save Filters Load Filters Topic""".lower()  # noqa: E501
    expected_fragment_keys = [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ADA_CODE",
            value=x,
        )
        for x in [
            "D2710",
            "D2720",
            "D2721",
            "D2722",
            "D2740",
            "D2750",
            "D2751",
            "D2752",
            "D2753",
            "D2780",
            "D2781",
            "D2782",
            "D2783",
            "D2790",
            "D2791",
            "D2792",
        ]
    ]
    expected_fragment_keys.sort(key=lambda x: x.value)
    actual_fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    actual_fragment_keys.sort(key=lambda x: x.value)
    assert actual_fragment_keys == expected_fragment_keys


def test_extractor_calc_fragment_keys_8eb156a290f14963a36a86ec6c5259d0_no_codes(
    artifact: ArtifactModel, fragment: FragmentModel
) -> None:
    extractor = Extractor()
    artifact.source_id = "8eb156a290f14963a36a86ec6c5259d0"
    artifact.external_id = "epolicies_20250407/content/Dental/Restorative_Codes/index.html"
    fragment.text_content = ""
    expected_fragment_keys = []
    actual_fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert actual_fragment_keys == expected_fragment_keys


def test_extractor_calc_fragment_keys_738ad2d781e3483cab3c55256ee0ac9b(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "738ad2d781e3483cab3c55256ee0ac9b"
    artifact.external_id = "epolicies_20250407/content/Dental/DR_Policies/DR_03_17.html"
    assert extractor.calc_fragment_keys(artifact, fragment) == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="DR_CODE",
            value="DR_03_17",
        )
    ]

    artifact.external_id = "epolicies_20250722/content/Dental/DR_Policies/DR_23_04_fedvip_cob_waiver.html"
    assert extractor.calc_fragment_keys(artifact, fragment) == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="DR_CODE",
            value="DR_23_04",
        )
    ]


def test_extractor_calc_fragment_keys_05814440726642c9b4f9f3f92aa9a5bf(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "05814440726642c9b4f9f3f92aa9a5bf"
    fragment.json_content = {"ADA_CD": "D6179", "PROCDTL_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ADA_CODE",
            value="D6179",
        ),
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PROCDTL_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_e673841c49d742a69515097bda1b4784(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "e673841c49d742a69515097bda1b4784"
    fragment.json_content = {"ADA_CD": "D6179", "ALTBNFT_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ADA_CODE",
            value="D6179",
        ),
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ALTBNFT_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_2a8f833fa363447ebb36a92315ce0e1a(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "2a8f833fa363447ebb36a92315ce0e1a"
    fragment.json_content = {"ALTBNFT_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ALTBNFT_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_ddc4d62f229244aa8888131f5e198f4c(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "ddc4d62f229244aa8888131f5e198f4c"
    fragment.json_content = {"PAYSCHD_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PAYSCHD_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_bf2cac489fb6454ea3a8456823c75b19(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "bf2cac489fb6454ea3a8456823c75b19"
    fragment.json_content = {"PLNDEAL_ID": "01000", "ADA_CD": "D0707"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ADA_CODE",
            value="D0707",
        ),
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PLNDEAL_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_c729a259374c4cccb72feacc73ce31f5(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "c729a259374c4cccb72feacc73ce31f5"
    fragment.json_content = {"PLNDEAL_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PLNDEAL_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_0c1155c8ed334ebabea86b4fba0fbd01(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "0c1155c8ed334ebabea86b4fba0fbd01"
    fragment.json_content = {"PAYSCHD_ID": "01000", "ADA_CD": "D0707"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ADA_CODE",
            value="D0707",
        ),
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PAYSCHD_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_5054a5c59eaf42fb9fe4230804b1fd9b(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "5054a5c59eaf42fb9fe4230804b1fd9b"
    fragment.json_content = {"ZP3SCHD_ID": "01000"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ZP3SCHD_ID",
            value="01000",
        ),
    ]


def test_extractor_calc_fragment_keys_d5896a4b38c94028842c310aab98fc79(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    artifact.source_id = "d5896a4b38c94028842c310aab98fc79"
    fragment.json_content = {"ZP3SCHD_ID": "01000", "PAYSCHD_ID": "01001"}
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == [
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="ZP3SCHD_ID",
            value="01000",
        ),
        FragmentKeyModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment.fragment_id,
            seq_no=fragment.seq_no,
            key="PAYSCHD_ID",
            value="01001",
        ),
    ]


def test_extractor_calc_fragment_keys_none(artifact: ArtifactModel, fragment: FragmentModel) -> None:
    extractor = Extractor()
    fragment_keys = extractor.calc_fragment_keys(artifact, fragment)
    assert fragment_keys == []


def test_calc_base_stop_words() -> None:
    base_stop_words = Extractor.calc_base_stop_words()
    assert len(base_stop_words) > 0
    assert all(map(lambda x: x.lower() == x, base_stop_words))


def test_calc_filter_text_content(fragment: FragmentModel) -> None:
    extractor = HTMLExtractor(MagicMock(), TextContentFilterModel(include_base_stop_words=True, additional_stop_words=["gonzo"]))
    raw_text_content = (
        "This is a Test of the filter: and but or not only "
        "Excluded data should be filtered, while significant words remain. "
        "Numbers like 123 and symbols! Gonzo gonzo"
    )
    filtered_text_content = extractor.filter_text_content(raw_text_content)
    base_stop_words = extractor.calc_base_stop_words()
    words = filtered_text_content.split(" ")
    assert all(map(lambda x: x.lower() == x, words))
    assert all(map(lambda x: len(x) > 1, words))
    assert all(map(lambda x: x not in base_stop_words, words))
    assert all(map(lambda x: x not in extractor.text_content_filter.additional_stop_words, words))
