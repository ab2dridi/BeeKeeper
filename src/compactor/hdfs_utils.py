from typing import Tuple


class HdfsUtils:
    """Simple HDFS utilities using Spark's JVM gateway.

    Methods assume `spark` is a SparkSession.
    This keeps operations as metadata moves when possible (rename).
    """

    def __init__(self, spark):
        self._spark = spark
        jvm = spark._jvm
        self._fs_class = jvm.org.apache.hadoop.fs.FileSystem
        self._path_class = jvm.org.apache.hadoop.fs.Path

    def _get_fs(self, uri=None):
        sc = self._spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        if uri:
            path = self._path_class(uri)
            return self._fs_class.get(path.toUri(), conf)
        return self._fs_class.get(conf)

    def exists(self, path: str) -> bool:
        p = self._path_class(path)
        fs = self._get_fs(path)
        return fs.exists(p)

    def rename(self, src: str, dst: str) -> bool:
        psrc = self._path_class(src)
        pdst = self._path_class(dst)
        fs = self._get_fs(src)
        return fs.rename(psrc, pdst)

    def delete(self, path: str, recursive: bool = True) -> bool:
        p = self._path_class(path)
        fs = self._get_fs(path)
        return fs.delete(p, recursive)

    def get_total_size_and_count(self, path: str) -> Tuple[int, int]:
        """Return (total_size_bytes, file_count) recursively under path."""
        p = self._path_class(path)
        fs = self._get_fs(path)
        total = 0
        count = 0
        iterator = fs.listFiles(p, True)
        while iterator.hasNext():
            status = iterator.next()
            if status.getLen() > 0:
                total += status.getLen()
                count += 1
        return total, count
