from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

conf = SparkConf()
conf.setAppName('pyspark-pandas=test')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

data = {"country": ["Brazil", "Russia", "India", "China", "South Africa"],
        "capital": ["Brasilia", "Moscow", "New Dehli", "Beijing", "Pretoria"],
        "area": [8.516, 17.10, 3.286, 9.597, 1.221],
        "population": [200.4, 143.5, 1252, 1357, 52.98] }

df_pd = pd.DataFrame(data, columns=data.keys())
df = sqlContext.createDataFrame(df_pd)
df.show()

sc.stop()
