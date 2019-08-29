# pyspark-helper

There are numerous settings and configurations to consider when running PySpark jobs on a YARN cluster. This script was designed to assist with the following:

- Create and capture an Anaconda virtual environment
- Set Environment Variables for Hadoop, Spark, and PySpark
- Call spark-submit with the minimum required parameters to instantiate a PySpark job on YARN

### Prerequisites

WARNING: This repo is intended for use in highly customized environments and will almost certainly have little to no value outside of those environments.

The following must be installed and configured in order to submit Pyspark Jobs against a YARN cluster:

- [Anaconda](https://www.anaconda.com/)
- [Hadoop](https://hadoop.apache.org/)
- [Spark](https://spark.apache.org/)

### SparkContext

Add the following to your python script to initialize SparkContext

```python
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

conf = SparkConf()
conf.setAppName('pyspark-test')
sc = SparkContext(conf=conf)
```

### Parameters

Regardless of whether you're running spark-submit or pyspark, the following parameters are appended by default

```bash
# command with default parameters
$ {command} \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./${venv}/bin/python" \
    --conf spark.yarn.dist.archives="file:///${project_venv}/${venv}-current.zip#${venv}" \
    $@
```

### Usage

By default this script will launch spark-submit and append your parameters, along with the python script to be ran, to the command.

```bash
# spark-submit pyspark script
$ ./pyspark-helper.sh ./examples/pyspark-pandas-test.py
```

If you would prefer an interactive shell instead, pass the --shell param to his helper script.

```bash
# launch interactive pyspark shell
$ ./pyspark-helper.sh --shell
```
