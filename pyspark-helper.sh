#!/usr/bin/env bash

#------------------------------------------------------------------------------
# FUNCTIONS
#------------------------------------------------------------------------------

# function: usage
function usage () {
  cat << EOF

  Usage: $0 /path/to/your/pyspark_script.py

    --shell   launches interactive pyspark shell
    --help    shows this help menu

  By default this script will launch spark-submit and append your parameters, 
  along with the python script to be ran, to the command. If you would prefer 
  an interactive shell instead, pass the --shell param to this helper script.

EOF
}

#------------------------------------------------------------------------------
# ARGUEMENTS
#------------------------------------------------------------------------------

# handle arguements
while getopts ":hs-:" FLAG; do
  case $FLAG in
    -)
      case ${OPTARG} in
        h|help)
          usage;
          exit 0;
          ;;
        s|shell)
          shell=true;
          ;;
      esac
      ;;
    h|help)
      usage;
      exit 0;
      ;;
    s|shell)
      shell=true;
      ;;
  esac
done
shift $((OPTIND-1))

#------------------------------------------------------------------------------
# VARIABLES
#------------------------------------------------------------------------------

project_dir="${HOME}/pyspark"
project_venv="${project_dir}/venvs"
epoch=$(date +%s)
condas=('numpy'
        'pandas')
pips=()
python='3.6.8'
venv="pyspark-${python}"
venv_updated=false
spark="2.3.0"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

#------------------------------------------------------------------------------
# ANACONDA
#------------------------------------------------------------------------------

# create working directories
if [ ! -d ${project_venv} ]; then
    echo -e "${GREEN}==> Creating PySpark Directories${NC}"
    mkdir -p "${project_venv}"
fi

# set anaconda env vars
if [ -f "/data/opt/anaconda/current/etc/profile.d/conda.sh" ]; then
    . "/data/opt/anaconda/current/etc/profile.d/conda.sh"
else
    export PATH="/data/opt/anaconda/current/bin:$PATH"
fi

# create anaconda venv if missing
if ! grep -q "${venv}" <(conda env list); then
    echo -e "${GREEN}==> Creating Anaconda ${venv} Virtual Environment${NC}"
    conda create -n "${venv}" -copy python=${python} anaconda -y
    venv_updated=true
fi
conda activate ${venv}

# install packages
echo -e "${GREEN}==> Adding Packages to Anaconda ${venv} Virtual Environment${NC}"
if [ "${#condas[@]}" -gt "0" ]; then
    conda_out=$(conda install --name ${venv} --copy ${condas[@]} | tee /dev/tty)
fi
if [ "${#pips[@]}" -gt "0" ]; then
    pip_out=$(python -m pip install ${pips[@]} | tee /dev/tty)
fi

# check if venv was modified
if [[ ! -z ${conda_out} ]] && [[ ! ${conda_out} =~ .*All\ requested\ packages\ already\ installed.* ]]; then
    venv_updated=true
fi
if [[ ! -z ${pip_out} ]] && [[ ! ${pip_out} =~ .*All\ requested\ packages\ already\ installed.* ]]; then
    venv_updated=true
fi

# zip venv
if [[ "${venv_updated}" == "true" ]]; then
    echo -e "${GREEN}==> Zipping Anaconda ${venv} Virtual Environment${NC}"
    venv_path="$(grep -o -P "^.*envs/" <(which python))${venv}"
    pushd ${venv_path}
    zip -r ${project_venv}/${venv}-${epoch}.zip .
    cd ${project_venv}
    rm -f "${project_venv}/${venv}-current.zip"
    ln -s ${venv}-${epoch}.zip ${venv}-current.zip
    popd
fi

#------------------------------------------------------------------------------
# ENV VARS
#------------------------------------------------------------------------------

# hadoop
export HADOOP_BIN_DIR="/usr/hdp/current/hadoop-client/bin"
export HADOOP_CONF_DIR="/usr/hdp/current/hadoop-client/conf"
export PATH="${HADOOP_BIN_DIR}:${PATH}"

# spark
export SPARK_HOME=/data/opt/spark/${spark}
export PATH="${SPARK_HOME}/bin:${PATH}"

# pyspark
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.6-src.zip"
export PYSPARK_DRIVER_PYTHON=`which python`
export PYSPARK_PYTHON=./${venv}/bin/python

#------------------------------------------------------------------------------
# SPARK
#------------------------------------------------------------------------------

if [ -f ${project_venv}/${venv}-current.zip ]; then
    echo -e "${GREEN}==> Launching PySpark in YARN${NC}"
    # select command
    if [[ "${shell}" == "true" ]]; then
      command='pyspark'
    else
      command='spark-submit'
    fi
    # execute command
    $command \
        --master yarn \
        --deploy-mode client \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./${venv}/bin/python" \
        --conf spark.yarn.dist.archives="file:///${project_venv}/${venv}-current.zip#${venv}" \
        $@
else
    echo -e "${RED}==> ERROR: Virtual Env Zip ${project_venv}/${venv}-current.zip Not Found${NC}"
fi
