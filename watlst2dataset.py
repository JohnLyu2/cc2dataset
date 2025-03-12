import fsspec
from loguru import logger
import pandas as pd
from pyspark.sql import SparkSession
from cc2dataset.spark_session_builder import build_spark_session
from cc2dataset.main import process_one_part, process_multi_part, process_wat, get_date_str


# SAMPLE_WAT_URL = f"https://data.commoncrawl.org/{SAMPLE_WAT_RELATIVE}"
WAT_LST_PATH = "cc_data/cc_main_2024_51/wat.paths"

def cc2dataset(
    output_path,
    # wat_count=None,
    master="local",
    num_cores=128,
    mem_gb=256,
    multipart=None,
    shuffle=True,
    resume=None,
    spark_builder=None,
    document_type="pdf",
    source_cc_protocol="http",
):
    """Convert common crawl to pdf url set"""

    assert resume is None, "Now do not support resume"

    if resume is not None and multipart is None:
        raise ValueError("Cannot resume without multipart")

    if resume is None:
        job_id = get_date_str()
        logger.info(f"JOB ID: {job_id}")
        output_path = f"{output_path}/{job_id}"
    else:
        output_path = resume

    logger.info(f"Writing in: {output_path}")

    if spark_builder is None:
        spark_builder = lambda: build_spark_session(master, num_cores, mem_gb)

    def build_spark():
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()
        return spark_builder()

    # if resume is None:
    #     wat_index_files = read_wat_index_files(wat_index_count, wat_count, source_cc_protocol)
    #     print(f"Jdebug: wat index files {wat_index_files}")
    #     # write wat index files to disk in output_path with fsspec
    #     with fsspec.open(f"{output_path}/wat_index_files.txt", "w", encoding="utf8") as f:
    #         f.write("\n".join(wat_index_files))
    # else:
    #     with fsspec.open(f"{output_path}/wat_index_files.txt", "r", encoding="utf8") as f:
    #         wat_index_files = f.read().splitlines()

    # read wat index files from WAT_LST_PATH
    wat_index_files = []
    with fsspec.open(WAT_LST_PATH, "r", encoding="utf8") as f:
        wat_index_files = f.read().splitlines()
    if multipart is None:
        process_one_part(output_path, wat_index_files, build_spark, shuffle, document_type, source_cc_protocol)
    else:
        process_multi_part(
            output_path, wat_index_files, build_spark, multipart, shuffle, resume, document_type, source_cc_protocol
        )


cc2dataset(
    output_path="outputs",
    master="local",
    num_cores=256,
    mem_gb=2048,
    multipart=90,
)
