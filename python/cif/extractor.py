from csv import DictReader, reader
from functools import cache
from io import StringIO
from logging import getLogger
from pathlib import Path
from re import findall, match, sub
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from bs4 import BeautifulSoup

from cif.connector import Connector
from cif.persistence import (
    AggregationLevel,
    ArtifactModel,
    FragmentKeyModel,
    FragmentModel,
    TextContentFilterModel,
)

logger = getLogger(__name__)


class Extractor:
    """
    Extract relevant content from an artifact.
    """

    def calc_fragments(self, artifact: ArtifactModel, **kwargs) -> List[FragmentModel]:
        raise NotImplementedError("calc_fragments")

    def calc_fragment_keys(self, artifact: ArtifactModel, fragment: FragmentModel) -> List[FragmentKeyModel]:
        # TODO https://cvsdigital.atlassian.net/browse/TLABCICA-2396
        if artifact.source_id == "8eb156a290f14963a36a86ec6c5259d0":
            # extract all Dxxxx keywords from the external_id (filename)
            external_id_ada_codes = findall(r"D\d{4}", artifact.external_id)
            if len(external_id_ada_codes) < 1:
                return []

            # find the lowest and highest code and treat as a range of potential
            # codes, then filter codes that appear in the text content against that
            # range. the intent is to remove Dxxxx codes that are merely referenced
            # in the text but are not part of the subject of the document
            external_id_ada_codes.sort()
            min_external_id_ada_code = int(external_id_ada_codes[0][1:])
            max_external_id_ada_code = int(external_id_ada_codes[-1][1:])
            subject_ada_codes = [f"d{i:04d}" for i in range(min_external_id_ada_code, 1 + max_external_id_ada_code)]
            text_content_ada_codes = set(findall(r"d\d{4}", fragment.text_content))
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ADA_CODE",
                    value=ada_code.upper(),
                )
                for ada_code in text_content_ada_codes
                if ada_code in subject_ada_codes
            ]
        elif artifact.source_id == "738ad2d781e3483cab3c55256ee0ac9b":
            basename = artifact.external_id.split("/")[-1].replace(".html", "")
            m = match(r"DR_\d{2}_\d{2}", basename)
            value = m[0] if m else basename
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="DR_CODE",
                    value=value,
                )
            ]
        elif artifact.source_id == "05814440726642c9b4f9f3f92aa9a5bf":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ADA_CODE",
                    value=fragment.json_content.get("ADA_CD"),
                ),
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PROCDTL_ID",
                    value=fragment.json_content.get("PROCDTL_ID"),
                ),
            ]
        elif artifact.source_id == "e673841c49d742a69515097bda1b4784":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ADA_CODE",
                    value=fragment.json_content.get("ADA_CD"),
                ),
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ALTBNFT_ID",
                    value=fragment.json_content.get("ALTBNFT_ID"),
                ),
            ]
        elif artifact.source_id == "2a8f833fa363447ebb36a92315ce0e1a":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ALTBNFT_ID",
                    value=fragment.json_content.get("ALTBNFT_ID"),
                )
            ]
        elif artifact.source_id == "ddc4d62f229244aa8888131f5e198f4c":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PAYSCHD_ID",
                    value=fragment.json_content.get("PAYSCHD_ID"),
                )
            ]
        elif artifact.source_id == "bf2cac489fb6454ea3a8456823c75b19":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ADA_CODE",
                    value=fragment.json_content.get("ADA_CD"),
                ),
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PLNDEAL_ID",
                    value=fragment.json_content.get("PLNDEAL_ID"),
                ),
            ]
        elif artifact.source_id == "c729a259374c4cccb72feacc73ce31f5":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PLNDEAL_ID",
                    value=fragment.json_content.get("PLNDEAL_ID"),
                ),
            ]
        elif artifact.source_id == "0c1155c8ed334ebabea86b4fba0fbd01":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ADA_CODE",
                    value=fragment.json_content.get("ADA_CD"),
                ),
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PAYSCHD_ID",
                    value=fragment.json_content.get("PAYSCHD_ID"),
                ),
            ]
        elif artifact.source_id == "5054a5c59eaf42fb9fe4230804b1fd9b":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ZP3SCHD_ID",
                    value=fragment.json_content.get("ZP3SCHD_ID"),
                ),
            ]
        elif artifact.source_id == "d5896a4b38c94028842c310aab98fc79":
            return [
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="ZP3SCHD_ID",
                    value=fragment.json_content.get("ZP3SCHD_ID"),
                ),
                FragmentKeyModel(
                    source_id=artifact.source_id,
                    artifact_id=artifact.artifact_id,
                    fragment_id=fragment.fragment_id,
                    seq_no=fragment.seq_no,
                    key="PAYSCHD_ID",
                    value=fragment.json_content.get("PAYSCHD_ID"),
                ),
            ]
        else:
            return []

    @classmethod
    @cache
    def calc_base_stop_words(cls) -> List[str]:
        """
        Compute the set of base stop words to use when processing Fragment text
        content.
        """
        with open(Path(__file__).parent / "base_stop_words.txt", "r") as f:
            base_stop_words = [x.strip().lower() for x in f.readlines() if x.strip() != ""]
            return base_stop_words

    def new_fragment(
        self,
        artifact: ArtifactModel,
        fragment_id: str,
        seq_no: int,
        aggregation_level: AggregationLevel,
        raw_text_content: str,
        json_content: Dict[str, Any],
    ) -> FragmentModel:
        """
        Create a new FragmentModel, filtering text if this Extractor is so configured.
        """
        text_content = self.filter_text_content(raw_text_content) if self.text_content_filter else raw_text_content
        return FragmentModel(
            source_id=artifact.source_id,
            artifact_id=artifact.artifact_id,
            fragment_id=fragment_id,
            aggregation_level=aggregation_level,
            seq_no=seq_no,
            text_content=text_content,
            json_content=json_content,
        )

    def filter_text_content(self, raw_text_content: str) -> str:
        """
        Filter the text_content field of the supplied Fragment object per the
        supplied configuration. Returns the text content with stop words, punctuation
        and certain other small words removed.
        """
        stop_words = self.calc_base_stop_words() if self.text_content_filter.include_base_stop_words else []
        stop_words += self.text_content_filter.additional_stop_words
        words = findall(r"\w+\-\w+|\w+", raw_text_content)
        words = [word.lower() for word in words if word.lower() not in stop_words and len(word) > 1]
        return " ".join(words)


class HTMLExtractor(Extractor):
    """
    Extracts content from HTML artifacts.
    """

    @classmethod
    def new_instance(cls, connector: Connector, **kwargs: Any) -> "HTMLExtractor":
        return cls(connector, **kwargs)

    def __init__(self, connector: Connector, text_content_filter: TextContentFilterModel = None) -> None:
        self.connector = connector
        self.text_content_filter = text_content_filter

    def parse_content(self, external_id: str) -> BeautifulSoup:
        chunks, _ = self.connector.get_artifact(external_id)
        content_bytes = b""
        for chunk in chunks:
            content_bytes += chunk
        content_text = content_bytes.decode("utf-8")
        return BeautifulSoup(content_text, "html.parser")

    def calc_fragments(self, artifact: ArtifactModel, **kwargs) -> List[FragmentModel]:
        """
        Return all text in the document, discarding markup and extraneous whitespace.
        """
        soup = self.parse_content(artifact.external_id)
        raw_text_content = sub(r"\s+", " ", soup.get_text(" ")).strip()
        return [self.new_fragment(artifact, uuid4().hex, 0, AggregationLevel.DOCUMENT, raw_text_content, None)]


class HTMLLinkExtractor(HTMLExtractor):
    def calc_fragments(self, artifact: ArtifactModel, **kwargs) -> List[Tuple[str, AggregationLevel]]:
        soup = self.parse_content(artifact.external_id)
        links = soup.find_all("a", href=True)
        return [
            self.new_fragment(
                artifact,
                uuid4().hex,
                0,
                AggregationLevel.LINK,
                f"{link['href']} {sub(r'\s+', ' ', link.get_text(' ').strip())}",
                None,
            )
            for seq_no, link in enumerate(links)
        ]


class HTMLTitleExtractor(HTMLExtractor):
    def calc_fragments(self, artifact: ArtifactModel, **kwargs) -> List[Tuple[str, AggregationLevel]]:
        soup = self.parse_content(artifact.external_id)
        title = soup.title
        return [self.new_fragment(artifact, uuid4().hex, 0, AggregationLevel.TITLE, title.get_text().strip(), None)]


class CSVRowExtractor(Extractor):
    @classmethod
    def new_instance(cls, connector: Connector, **kwargs: Any) -> "CSVRowExtractor":
        return cls(connector)

    def __init__(self, connector: Connector, text_content_filter: TextContentFilterModel = None) -> None:
        self.connector = connector
        self.text_content_filter = text_content_filter

    def parse_content(self, external_id: str, start_byte: Optional[int] = None, end_byte: Optional[int] = None) -> StringIO:
        if start_byte is not None and end_byte is not None:
            chunks = [self.connector.get_artifact_chunk(external_id, start_byte, end_byte)]
            logger.debug("Read bytes %d - %d of %s", start_byte, end_byte, external_id)
        else:
            chunks, _ = self.connector.get_artifact(external_id)
        content = b""
        for chunk in chunks:
            content += chunk
        return StringIO(content.decode("utf-8"))

    def calc_fieldnames(self, artifact: ArtifactModel) -> List[str]:
        line = self.connector.get_artifact_chunk(artifact.external_id, 0, 4096)
        r = StringIO(line.decode("utf-8"))
        return next(reader(r))

    def calc_fragments(
        self, artifact: ArtifactModel, fragment_id: str = None, start_byte: int = None, end_byte: int = None
    ) -> List[FragmentModel]:
        fieldnames = self.calc_fieldnames(artifact)
        f = self.parse_content(artifact.external_id, start_byte=start_byte, end_byte=end_byte)
        reader = DictReader(f, fieldnames=fieldnames)
        fragments = []
        seq_no = 0
        for row in reader:
            fragments.append(self.new_fragment(artifact, uuid4().hex, 0, AggregationLevel.ROW, " ".join(row.values()), row))
            seq_no += 1
        return fragments
