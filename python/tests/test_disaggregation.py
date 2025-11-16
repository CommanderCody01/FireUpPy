from unittest.mock import MagicMock, call, patch

from pytest import fixture

from cif.disaggregation import DeferredDisaggregationModel, Disaggregation
from cif.persistence import (
    AggregationLevel,
    ArtifactModel,
    DisaggregationMode,
    FilesystemConnectorModel,
    FragmentKeyModel,
    FragmentModel,
    GenerationModel,
    HTMLExtractorModel,
    SourceModel,
)
from cif.util import now


@fixture
def source() -> SourceModel:
    return SourceModel(
        source_id="source_id",
        external_id="external_id",
        category="category",
        created_on=now(),
        enabled=True,
        connector=FilesystemConnectorModel(root="../local/test_100/0", glob_pattern="*.txt"),
        extractors=[HTMLExtractorModel()],
        disaggregation_mode=DisaggregationMode.DEFERRED,
        retain_generations=3,
    )


@fixture
def artifact() -> ArtifactModel:
    return ArtifactModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        version="version",
        external_id="external_id",
        created_on=now(),
        content_length=0,
        content_type="text/html",
    )


def test_defer_all(source: SourceModel, artifact: ArtifactModel) -> None:
    generation = MagicMock()
    artifacts = [artifact] * 105  # two pages of calls
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock()])
    disaggregation.do_publish_messages = MagicMock()
    disaggregation.defer_all(generation, artifacts)
    disaggregation.do_publish_messages.assert_has_calls(
        [
            call(
                [
                    DeferredDisaggregationModel(
                        source_id=source.source_id,
                        generation_id=generation.generation_id,
                        artifact_id=artifact.artifact_id,
                        extractor_type=extractor.type,
                        created_on=generation.created_on,
                    )
                    for artifact in artifacts
                    for extractor in source.extractors
                ]
            ),
        ]
    )


def test_defer_and_chunk_all(source: SourceModel, artifact: ArtifactModel) -> None:
    generation = MagicMock()
    artifacts = [artifact]
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock()])
    disaggregation.do_publish_messages = MagicMock()
    # expect the artifact to break down into 3 chunks of 50k lines per, thus
    # creating three disaggregations
    disaggregation.connector.calc_line_chunks.return_value = [(0, 49999), (50000, 99999), (100000, 149999)]
    with patch("cif.disaggregation.uuid4") as mock_uuid4:
        mock_uuid4.return_value.hex = "fragment_id"
        disaggregation.defer_and_chunk_all(generation, artifacts)
    disaggregation.connector.calc_line_chunks.assert_called_once_with(artifact.external_id, 50000)
    disaggregation.do_publish_messages.assert_has_calls(
        [
            call(
                [
                    DeferredDisaggregationModel(
                        source_id=source.source_id,
                        generation_id=generation.generation_id,
                        artifact_id=artifact.artifact_id,
                        extractor_type=source.extractors[0].type,
                        created_on=generation.created_on,
                        start_byte=0,
                        end_byte=49999,
                        fragment_id="fragment_id",
                    ),
                    DeferredDisaggregationModel(
                        source_id=source.source_id,
                        generation_id=generation.generation_id,
                        artifact_id=artifact.artifact_id,
                        extractor_type=source.extractors[0].type,
                        created_on=generation.created_on,
                        start_byte=50000,
                        end_byte=99999,
                        fragment_id="fragment_id",
                    ),
                    DeferredDisaggregationModel(
                        source_id=source.source_id,
                        generation_id=generation.generation_id,
                        artifact_id=artifact.artifact_id,
                        extractor_type=source.extractors[0].type,
                        created_on=generation.created_on,
                        start_byte=100000,
                        end_byte=149999,
                        fragment_id="fragment_id",
                    ),
                ]
            ),
        ]
    )


def test_disaggregate_and_chunk_all(source: SourceModel, artifact: ArtifactModel) -> None:
    generation = MagicMock()
    artifacts = [artifact]
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock()])
    disaggregation.disaggregate_one = MagicMock()
    # expect the artifact to break down into 3 chunks of 50k lines per, thus
    # creating three disaggregations
    disaggregation.connector.calc_line_chunks.return_value = [(0, 49999), (50000, 99999), (100000, 149999)]
    with patch("cif.disaggregation.uuid4") as mock_uuid4:
        mock_uuid4.return_value.hex = "fragment_id"
        disaggregation.disaggregate_and_chunk_all(generation, artifacts)
    disaggregation.connector.calc_line_chunks.assert_called_once_with(artifact.external_id, 50000)
    disaggregation.disaggregate_one.assert_has_calls(
        [
            call(generation, artifact, disaggregation.extractors[0], "fragment_id", 0, 49999),
            call(generation, artifact, disaggregation.extractors[0], "fragment_id", 50000, 99999),
            call(generation, artifact, disaggregation.extractors[0], "fragment_id", 100000, 149999),
        ],
        any_order=True,
    )


def test_disaggregate_deferred(source: SourceModel, artifact: ArtifactModel) -> None:
    source.disaggregation_mode = DisaggregationMode.DEFERRED
    generation = GenerationModel(source_id=source.source_id, generation_id=0, created_on=now())
    artifacts = [artifact] * 105
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.catalog.get_new_artifacts.side_effect = [(artifacts[0:100], 100), (artifacts[100:], None)]
    disaggregation.defer_all = MagicMock()
    disaggregation.defer_all.return_value = len(artifacts) * len(source.extractors)
    disaggregation.disaggregate(generation)
    disaggregation.defer_all.assert_has_calls([call(generation, artifacts[0:100]), call(generation, artifacts[100:])])


def test_disaggregate_deferred_chunked(source: SourceModel, artifact: ArtifactModel) -> None:
    source.disaggregation_mode = DisaggregationMode.DEFERRED_CHUNKED
    generation = GenerationModel(source_id=source.source_id, generation_id=0, created_on=now())
    artifacts = [artifact] * 105
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.catalog.get_new_artifacts.side_effect = [(artifacts[0:100], 100), (artifacts[100:], None)]
    disaggregation.defer_and_chunk_all = MagicMock()
    disaggregation.defer_and_chunk_all.return_value = len(artifacts) * len(source.extractors)
    disaggregation.disaggregate(generation)
    disaggregation.defer_and_chunk_all.assert_has_calls([call(generation, artifacts[0:100]), call(generation, artifacts[100:])])


def test_disaggregate_immediate_chunked(source: SourceModel, artifact: ArtifactModel) -> None:
    source.disaggregation_mode = DisaggregationMode.IMMEDIATE_CHUNKED
    generation = GenerationModel(source_id=source.source_id, generation_id=0, created_on=now())
    artifacts = [artifact] * 105
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.catalog.get_new_artifacts.side_effect = [(artifacts[0:100], 100), (artifacts[100:], None)]
    disaggregation.disaggregate_and_chunk_all = MagicMock()
    disaggregation.disaggregate_and_chunk_all.return_value = len(artifacts) * len(source.extractors)
    disaggregation.disaggregate(generation)
    disaggregation.disaggregate_and_chunk_all.assert_has_calls(
        [call(generation, artifacts[0:100]), call(generation, artifacts[100:])]
    )


def test_disaggregate_immediate(source: SourceModel, artifact: ArtifactModel) -> None:
    source.disaggregation_mode = DisaggregationMode.IMMEDIATE
    generation = GenerationModel(source_id=source.source_id, generation_id=0, created_on=now())
    artifacts = [artifact] * 105
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.catalog.get_new_artifacts.side_effect = [(artifacts[0:100], 100), (artifacts[100:], None)]
    disaggregation.disaggregate_one = MagicMock()
    disaggregation.disaggregate_one.return_value = 1
    disaggregation.disaggregate(generation)
    disaggregation.disaggregate_one.assert_has_calls(
        [call(generation, artifact, extractor) for artifact in artifacts for extractor in disaggregation.extractors]
    )


def test_disaggregate_one(source: SourceModel, artifact: ArtifactModel) -> None:
    generation = GenerationModel(source_id=source.source_id, generation_id=0, created_on=now())
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.catalog.insert_fragments.return_value = 1
    extractor = MagicMock()
    with patch("cif.extractor.uuid4") as mock_uuid4:
        mock_uuid4.return_value.hex = "fragment_id"
        extractor.calc_fragments.return_value = [
            FragmentModel(
                source_id=source.source_id,
                artifact_id=artifact.artifact_id,
                fragment_id=mock_uuid4().hex,
                text_content="text_content",
                seq_no=0,
                aggregation_level=AggregationLevel.DOCUMENT,
                logical_id="logical_id",
            )
        ]
        extractor.calc_fragment_keys.return_value = [
            FragmentKeyModel(
                source_id=artifact.source_id,
                artifact_id=artifact.artifact_id,
                fragment_id=mock_uuid4.return_value.hex,
                seq_no=0,
                key="KEY",
                value="VALUE",
            )
        ]
        count = disaggregation.disaggregate_one(generation, artifact, extractor)
        assert count == 1
        extractor.calc_fragments.assert_called_once_with(artifact, fragment_id=None, start_byte=None, end_byte=None)
        disaggregation.catalog.insert_fragments.assert_called_once_with(extractor.calc_fragments.return_value)


@patch("cif.disaggregation.wait")
def test_do_publish_messages(mock_wait: MagicMock, source: SourceModel, artifact: ArtifactModel) -> None:
    disaggregation = Disaggregation(MagicMock(), MagicMock(), MagicMock(), source, [MagicMock() for x in source.extractors])
    disaggregation.pub.publish_messages.return_value = [MagicMock()]
    msgs = [
        DeferredDisaggregationModel(
            source_id=source.source_id,
            generation_id=0,
            artifact_id=artifact.artifact_id,
            extractor_type="extractor_type",
            created_on=now(),
        )
    ]
    disaggregation.do_publish_messages(msgs)
    disaggregation.catalog.insert_deferred_disaggregations.assert_has_calls([call(msgs)])
    disaggregation.pub.publish_messages.assert_has_calls([call([msg.model_dump_json().encode("utf-8") for msg in msgs])])
    mock_wait.assert_has_calls([call(disaggregation.pub.publish_messages.return_value)])
