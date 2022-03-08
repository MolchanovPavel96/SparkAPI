# -*- coding: utf-8 -*-

# Алгоритм регистрации пользовательской функции в PySpark (UDF - User-Defined Function)
# на примере функции mask_hash() с целью взятия хеша от строки

import re
import hashlib
from pyspark.sql import functions
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType

# Функция хеширования в виде лямбда-функции
mask_hash = lambda value: hashlib.sha256(value.encode(encoding="utf-8")).hexdigest()

# UDF
spark_udf = udf(mask_hash, StringType())

# Датафрейм с полем, от которого надо взять хеш
fio_df = spark.sql("select 'Molchanov.P.V.' as fio")

# Добавляем в датафрейм новое поле с результатом хеширования
result_df = fio_df.withColumn("fio_hash", spark_udf(functions.col("fio")))

result_df.show()

# UDF regexp_like()
regexp_like = lambda expression, pattern: (expression == re.match(pattern, expression).group(0)) if (re.match(pattern, expression) is not None) else False

regexp_udf = udf(regexp_like, BooleanType())

host_df = spark.sql("select 'address123.com' as host")

host_df.withColumn("regexp_like", regexp_udf(functions.col("host"), functions.lit("^(\d|[A-z])+\.(\d|[A-z])+$"))).show()
