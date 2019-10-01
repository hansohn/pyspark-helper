import pandas as pd
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName('pyspark-pandas-test')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

data = {"country": ["Brazil", "Russia", "India", "China", "South Africa"],
        "capital": ["Brasilia", "Moscow", "New Dehli", "Beijing", "Pretoria"],
        "area": [8.516, 17.10, 3.286, 9.597, 1.221],
        "population": [200.4, 143.5, 1252, 1357, 52.98] }

pd_df = pd.DataFrame(data, columns=data.keys())
df = sqlContext.createDataFrame(pd_df)
df.show()
