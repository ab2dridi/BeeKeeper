"""Wrapper entry point for spark-submit.

Usage:
    spark-submit \\
        --master yarn \\
        --deploy-mode client \\
        --archives beekeeper_env.tar.gz#beekeeper_env \\
        --conf spark.pyspark.python=./beekeeper_env/bin/python \\
        run_lakekeeper.py compact --database mydb --block-size 128
"""

from lakekeeper.cli import main

if __name__ == "__main__":
    main()
