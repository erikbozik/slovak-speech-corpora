from src.schemas import DataMetaData

VOXPOPULI_METADATA = [
    DataMetaData(
        data_path="data/voxpopuli/asr_train.tsv",
        audio_dir_path="data/voxpopuli/train",
        source_part="train",
    ),
    DataMetaData(
        data_path="data/voxpopuli/asr_dev.tsv",
        audio_dir_path="data/voxpopuli/dev",
        source_part="dev",
    ),
    DataMetaData(
        data_path="data/voxpopuli/asr_test.tsv",
        audio_dir_path="data/voxpopuli/test",
        source_part="test",
    ),
]

COMMON_VOICE_METADATA = [
    # DataMetaData(
    #     data_path="data/common_voice/invalidated.tsv",
    #     audio_dir_path="data/common_voice/clips",
    #     source_part="invalidated",
    # ),
    DataMetaData(
        data_path="data/common_voice/validated.tsv",
        audio_dir_path="data/common_voice/clips",
        source_part="validated",
    ),
    # DataMetaData(
    #     data_path="data/common_voice/dev.tsv",
    #     audio_dir_path="data/common_voice/clips",
    #     source_part="dev",
    # ),
    # DataMetaData(
    #     data_path="data/common_voice/test.tsv",
    #     audio_dir_path="data/common_voice/clips",
    #     source_part="test",
    # ),
    # DataMetaData(
    #     data_path="data/common_voice/train.tsv",
    #     audio_dir_path="data/common_voice/clips",
    #     source_part="train",
    # ),
]
