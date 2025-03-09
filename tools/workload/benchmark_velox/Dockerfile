ARG SPARK_VERSION

#/////////////////////////////////////////////
#          gluten-cpp-static-image
#//////////////////////////////////////////////
# apache/gluten:vcpkg-centos-7 is used by all OSS Gluten CI test
FROM apache/gluten:vcpkg-centos-7 AS gluten-cpp-image-static

# Build Gluten Jar

ENV GCC_TOOL=/opt/rh/devtoolset-11

RUN cd ~/ && \
    wget https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz &>/dev/null && \
    tar xvf openjdk-17.0.2_linux-x64_bin.tar.gz  &>/dev/null && \
    mv jdk-17.0.2 /usr/lib/jvm/ && \
    echo "export JAVA_HOME=/usr/lib/jvm/jdk-17.0.2" > /root/.javacfg && \
    source /root/.javacfg && \
    update-alternatives --install "/usr/bin/javac" "javac" "${JAVA_HOME}/bin/javac" 2 && \
    update-alternatives --install "/usr/bin/java" "java" "${JAVA_HOME}/bin/java" 2 && \
    update-alternatives --set java ${JAVA_HOME}/bin/java && \
    update-alternatives --set javac ${JAVA_HOME}/bin/javac && \
    rm -rf openjdk-17.0.2_linux-x64_bin.tar.gz && \
    source ${GCC_TOOL}/enable && \
    it clone --depth 1 https://github.com/apache/incubator-gluten.git && \
    cd /incubator-gluten && \
    du -s /root/.cache/ccache && \
    ./dev/builddeps-veloxbe.sh --run_setup_script=OFF --enable_s3=ON --enable_gcs=ON --enable_abfs=ON --enable_vcpkg=ON --build_arrow=OFF --velox_home=/velox && \
    du -s /root/.cache/ccache && \
    ccache -s -V

#/////////////////////////////////////////////
#          gluten-jar-image
#//////////////////////////////////////////////
FROM gluten-cpp-image-static AS gluten-image
ARG SPARK_VERSION

RUN echo "Spark version: ${SPARK_VERSION}" && \
    source /root/.javacfg && \
    alternatives --set java ${JAVA_HOME}/bin/java && \
    alternatives --set javac ${JAVA_HOME}/bin/javac && \
    source ${GCC_TOOL}/enable && \
    cd incubator-gluten && java --version && \
    mvn clean package -Pbackends-velox -Piceberg -Pdelta -Pspark-${SPARK_VERSION} -DskipTests


#/////////////////////////////////////////////
#          gluten-gen-data
#//////////////////////////////////////////////
FROM quay.io/centos/centos:stream9 AS gen_data

RUN set ex; \
    curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo; \
    mv sbt-rpm.repo /etc/yum.repos.d/; \
    dnf install -y sbt java-17-openjdk git wget make gcc g++ byacc flex bison; \
    git clone https://github.com/databricks/spark-sql-perf.git ~/spark-sql-perf; \
    cd ~/spark-sql-perf; \
    sbt package; \
    mkdir -p /opt/datagen; \
    cp ~/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar /opt/datagen/; \
    git clone https://github.com/databricks/tpch-dbgen.git /opt/datagen/tpch-dbgen; \
    cd /opt/datagen/tpch-dbgen; \
    git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8; \
    make clean; \
    make OS=LINUX; \
    git clone https://github.com/databricks/tpcds-kit.git /opt/datagen/tpcds-kit; \
    cd /opt/datagen/tpcds-kit/tools; \
    make clean && make OS=LINUX;


COPY gen_tpch.py /opt/datagen/
COPY gen_tpcds.py /opt/datagen/


#/////////////////////////////////////////////
#          gluten-runtime
#//////////////////////////////////////////////

FROM quay.io/centos/centos:stream9 AS deploy-image

ARG ARG_SPARK_TGZ_URL
ARG ARG_SPARK_TGZ_ASC_URL
ARG ARG_GPG_KEY
ARG ARG_ICEBERG_URL

RUN set -ex; \
    useradd spark; \
    dnf install -y bash vim libssh gnupg2 hostname java-17-openjdk less net-tools procps python3 sudo util-linux wget which pip telegraf; \
    dnf install -y epel-release; \
    dnf install -y jemalloc; \
    ln -s $(which python3) /usr/bin/python; \
    python3 -m pip install notebook==6.5.2; \
    python3 -m pip install jupyter_server==1.23.4; \
    python3 -m pip install jupyter_highlight_selected_word; \
    python3 -m pip install jupyter_contrib_nbextensions; \
    python3 -m pip install virtualenv==20.21.1; \
    python3 -m pip uninstall -y ipython; \
    python3 -m pip install ipython==8.18.0; \
    python3 -m pip uninstall -y traitlets; \
    python3 -m pip install traitlets==5.9.0; \
    python3 -m pip install --upgrade jsonschema; \
    python3 -m pip install jsonschema[format]; \
    python3 -m pip install sqlalchemy==1.4.46; \
    python3 -m pip install papermill Black; \
    python3 -m pip install NotebookScripter; \
    python3 -m pip install findspark matplotlib pyhdfs; \
    python3 -m pip install ipywidgets jupyter_nbextensions_configurator ipyparallel; \
    python3 -m pip install jupyter_client===7.4.9; \
    python3 -m pip install jupyter-events===0.10.0; \
    python3 -m pip install jupyter_server==2.14.1; \
    python3 -m pip install jupyter-nbextensions-configurator==0.6.3; \
    python3 -m pip install 'googleapis-common-protos>=1.56.4'; \
    python3 -m pip install 'grpcio-status>=1.48.1'; \
    python3 -m pip install 'grpcio>=1.48.1'; \
    python3 -m pip install 'numpy<2,>=1.15'; \
    python3 -m pip install 'pandas==1.5.3'; \
    python3 -m pip install 'pandasql==0.7.3'; \
    python3 -m pip install 'pyarrow>=1.0.0'; \
    python3 -m pip install sqlparse; \
    python3 -m pip install seaborn; \
    python3 -m pip install boto3; \
    dnf clean all; \
    mkdir -p /opt/spark/work-dir; \
    mkdir -p /opt/spark/work-dir/ipython/output; \
    mkdir -p /opt/spark/work-dir/ipython/result; \
    mkdir -p /opt/spark/work-dir/ipython/analysis; \
    chmod g+w /opt/spark/work-dir; \
    mkdir -p /opt/spark/events; \
    chmod g+w /opt/spark/events; \
    export SPARK_TMP="$(mktemp -d)"; \
    cd $SPARK_TMP; \
    wget -nv -O spark.tgz "$ARG_SPARK_TGZ_URL"; \
    wget -nv -O spark.tgz.asc "$ARG_SPARK_TGZ_ASC_URL"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver hkps://keys.openpgp.org --recv-key "$GPG_KEY" || gpg --batch --keyserver hkps://keyserver.ubuntu.com --recv-keys "$ARG_GPG_KEY"; \
    gpg --batch --verify spark.tgz.asc spark.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" spark.tgz.asc; \
    tar -xf spark.tgz --strip-components=1; \
    chown -R spark:spark .; \
    mv RELEASE  /opt/spark/; \
    mv bin      /opt/spark/; \
    mv conf     /opt/spark/; \
    mv data     /opt/spark/; \
    mv examples /opt/spark/; \
    mv jars     /opt/spark/; \
    mv python   /opt/spark/; \
    mv sbin     /opt/spark/; \
    cd ..; \
    rm -rf "$SPARK_TMP"; \
    cd /opt/spark/jars; \
    wget -nv https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    wget -nv https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    wget -nv $ARG_ICEBERG_URL; \
    cd /opt/spark; \
    chown -R spark:spark .; \
    echo 'spark ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers; \
    echo 'alias ll="ls -alh"' >> /etc/bashrc
    

COPY ipython /opt/spark/work-dir/ipython
COPY --chmod=0775 --from=gluten-image /incubator-gluten/package/target/gluten-velox-bundle-*.jar /opt/gluten/jars/
COPY entrypoint.sh /opt/entrypoint.sh
COPY --chmod=0775 --from=gen_data /opt/datagen /opt/datagen

ENV SPARK_HOME=/opt/spark \
    SPARK_NO_DAEMONIZE=true \
    PATH=$SPARK_HOME/bin:$PATH

WORKDIR /opt/spark/work-dir

USER spark
RUN jupyter notebook --generate-config; \
    mkdir -p ~/.jupyter/custom/; \
    echo '.container { width:100% !important; }' >> ~/.jupyter/custom/custom.css; \
    echo 'div.output_stderr { background: #ffdd; display: none; }'  >> ~/.jupyter/custom/custom.css; \
    echo '.CodeMirror{font-family: "Courier New";font-size: 12pt;}' >> ~/.jupyter/custom/custom.css; \
    echo '.rendered_html table, .rendered_html td, .rendered_html th {font-size: 20px;}' >> ~/.jupyter/custom/custom.css; \
    jupyter nbextension install --py jupyter_highlight_selected_word --user; \
    jupyter nbextension enable highlight_selected_word/main; \
    jupyter nbextension install --py widgetsnbextension --user; \
    jupyter contrib nbextension install --user; \
    jupyter nbextension enable codefolding/main; \
    jupyter nbextension enable code_prettify/code_prettify; \
    jupyter nbextension enable codefolding/edit; \
    jupyter nbextension enable code_font_size/code_font_size; \
    jupyter nbextension enable collapsible_headings/main; \
    jupyter nbextension enable highlight_selected_word/main; \
    jupyter nbextension enable ipyparallel/main; \
    jupyter nbextension enable move_selected_cells/main; \
    jupyter nbextension enable nbTranslate/main; \
    jupyter nbextension enable scratchpad/main; \
    jupyter nbextension enable tree-filter/index; \
    jupyter nbextension enable comment-uncomment/main; \
    jupyter nbextension enable export_embedded/main; \
    jupyter nbextension enable hide_header/main; \
    jupyter nbextension enable highlighter/highlighter; \
    jupyter nbextension enable scroll_down/main; \
    jupyter nbextension enable snippets/main; \
    jupyter nbextension enable toc2/main; \
    jupyter nbextension enable varInspector/main; \
    jupyter nbextension enable codefolding/edit; \
    jupyter nbextension enable contrib_nbextensions_help_item/main; \
    jupyter nbextension enable freeze/main; \
    jupyter nbextension enable hide_input/main; \
    jupyter nbextension enable jupyter-js-widgets/extension; \
    jupyter nbextension enable snippets_menu/main; \
    jupyter nbextension enable table_beautifier/main; \
    jupyter nbextension enable hide_input_all/main; \
    jupyter nbextension enable spellchecker/main; \
    jupyter nbextension enable toggle_all_line_numbers/main; \
    jupyter nbextensions_configurator enable --user;
    
ENTRYPOINT ["/opt/entrypoint.sh"]



