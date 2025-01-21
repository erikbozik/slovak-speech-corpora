from src.schemas import DataMetaData

VOXPOPULI_METADATA = [
    DataMetaData(
        tsv_path="voxpopuli/asr_train.tsv",
        audio_dir_path="voxpopuli/train",
        source_part="train",
    ),
    DataMetaData(
        tsv_path="voxpopuli/asr_dev.tsv",
        audio_dir_path="voxpopuli/dev",
        source_part="dev",
    ),
    DataMetaData(
        tsv_path="voxpopuli/asr_test.tsv",
        audio_dir_path="voxpopuli/test",
        source_part="test",
    ),
]
