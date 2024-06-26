#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

function build_for_spark {
  spark_version=$1
  mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-$spark_version -DskipTests -Dcheckstyle.skip=true -DskipScalastyle=true
}

cd $GLUTEN_DIR

build_for_spark 3.2
