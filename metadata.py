from src.schemas import DataMetaData

VOXPOPULI_METADATA = [
    DataMetaData(
        data_path="voxpopuli/asr_train.tsv",
        audio_dir_path="voxpopuli/train",
        source_part="train",
    ),
    DataMetaData(
        data_path="voxpopuli/asr_dev.tsv",
        audio_dir_path="voxpopuli/dev",
        source_part="dev",
    ),
    DataMetaData(
        data_path="voxpopuli/asr_test.tsv",
        audio_dir_path="voxpopuli/test",
        source_part="test",
    ),
]

COMMON_VOICE_METADATA = [
    # DataMetaData(
    #     data_path="common_voice/invalidated.tsv",
    #     audio_dir_path="common_voice/clips",
    #     source_part="invalidated",
    # ),
    DataMetaData(
        data_path="common_voice/validated.tsv",
        audio_dir_path="common_voice/clips",
        source_part="validated",
    ),
    DataMetaData(
        data_path="common_voice/dev.tsv",
        audio_dir_path="common_voice/clips",
        source_part="dev",
    ),
    DataMetaData(
        data_path="common_voice/test.tsv",
        audio_dir_path="common_voice/clips",
        source_part="test",
    ),
    DataMetaData(
        data_path="common_voice/train.tsv",
        audio_dir_path="common_voice/clips",
        source_part="train",
    ),
]
