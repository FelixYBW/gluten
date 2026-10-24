---
layout: page
title: Pre-built Docker Images
nav_order: 22
parent: Developer Overview
---
# Pre-built Docker Images

Gluten publishes pre-built images to Docker Hub under
[`apache/gluten`](https://hub.docker.com/r/apache/gluten/tags). CI jobs use them as build and test
environments, and they also work as local development environments. Every image is built from a
Dockerfile under [`dev/docker/`](https://github.com/apache/gluten/tree/main/dev/docker) by the
[`docker_image.yml`](https://github.com/apache/gluten/blob/main/.github/workflows/docker_image.yml)
workflow.

## Images

Sizes are the compressed size of one architecture, as reported by Docker Hub for the 2026-10-18
build, unless marked otherwise. Pulled images take about twice that on disk.

| Tag | Dockerfile | Arch | Size | Used by |
|---|---|---|---|---|
| [`vcpkg-almalinux-8-gcc13`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-almalinux-8-gcc13) | [`Dockerfile.almalinux8-gcc13-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.almalinux8-gcc13-static-build) | amd64, arm64 | 1.7 GB | Static native builds in `velox_backend_x86`, `delta_spark_ut`, `velox_kafka`, `velox_native_build` (Iceberg), `velox_backend_ansi`, `velox_backend_enhanced`, `velox_nightly`, `velox_backend_cache`, `build_bundle_package` and `build_release`; [release builds](../../../developers/HowToRelease.md); [static build in Docker](velox-backend-build-in-docker.md) |
| [`vcpkg-centos-8-gcc13`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-centos-8-gcc13) | [`Dockerfile.centos8-gcc13-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-gcc13-static-build) | amd64, arm64 | 2.7 GB | Not used by any workflow |
| [`vcpkg-centos-9`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-centos-9) | [`Dockerfile.centos9-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-static-build) | amd64, arm64 | 2.1 GB | Static native build in `velox_backend_arm`; `velox_backend_cache`; the static [dev container](../../../developers/dev-container.md) |
| [`centos-8-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk8) | [`Dockerfile.centos8-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-dynamic-build) | amd64, arm64 | 5.5 GB | Spark 3.4 / 3.5 unit tests and the Uniffle TPC job in `velox_backend_x86`; `velox_backend_enhanced`; `velox_nightly`; [dynamic build in Docker](velox-backend-build-in-docker.md) |
| [`centos-8-jdk11`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk11) | same, `JAVA_VERSION=11` | amd64, arm64 | 5.5 GB | Not used by any workflow |
| [`centos-8-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk17) | same, `JAVA_VERSION=17` | amd64, arm64 | 5.5 GB | Spark 4.0 unit tests and build checks in `velox_backend_x86`; `velox_backend_enhanced`; `flink` |
| [`centos-9-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk8) | [`Dockerfile.centos9-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-dynamic-build) | amd64, arm64 | 5.1 GB | `cpp-test-udf-test` (shared build, C++ tests, clang-tidy) in `velox_backend_x86` and `velox_backend_arm`; `velox_backend_cache`; the default [dev container](../../../developers/dev-container.md) |
| [`centos-9-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17) | same, `JAVA_VERSION=17` | amd64, arm64 | 5.1 GB | Spark 4.1 / 4.2 unit tests in `velox_backend_x86`; `delta_spark_ut`; `velox_backend_ansi`; the Iceberg test shards (`iceberg_upstream_revision`) |
| [`centos-9-jdk17-cuda13.1-cudf`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17-cuda13.1-cudf) | [`cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf`](https://github.com/apache/gluten/blob/main/dev/docker/cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf) | amd64 | 13.2 GB | `build-cudf-centos-9` in `velox_backend_x86`; `velox_backend_cache`; [GPU support](../get-started/VeloxGPU.md) |
| [`centos-8-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-runtime) | [`Dockerfile.centos8-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-runtime) | amd64 | 1.9 GB\* | `tpc-test-centos8` and the Celeborn TPC job in `velox_backend_x86` |
| [`centos-9-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-runtime) | [`Dockerfile.centos9-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-runtime) | amd64, arm64 | 1.8 GB\* | `tpc-test-centos8` in `velox_backend_arm` |
| [`ubuntu-22.04-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=ubuntu-22.04-runtime) | [`ubuntu/Dockerfile.ubuntu22-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/ubuntu/Dockerfile.ubuntu22-runtime) | amd64 | 3.4 GB\* | `tpc-test-ubuntu` in `velox_backend_x86` |

\* Uncompressed size from a local build; Docker Hub sizes appear after the first publish.

Pull an image with `docker pull apache/gluten:<tag>`, for example:

```bash
docker pull apache/gluten:vcpkg-almalinux-8-gcc13
docker run -it --rm -v "$PWD":/work -w /work apache/gluten:vcpkg-almalinux-8-gcc13 bash
```

Docker picks the matching architecture of a multi-arch tag automatically.

## What the images contain

The build images clone Gluten to pre-populate dependencies, then delete the clone and the ccache, so
no image contains Gluten source or binaries.

Every image also ships the tools CI jobs need at runtime, so jobs do not install packages and do not
depend on package mirrors. `dev/docker/install-ci-tools.sh` installs `gh` and `jq` (needed by the
Apache Stash cache actions) into every image except `vcpkg-centos-8-gcc13` and the GPU images.

### Static build images (`vcpkg-*`)

These build `libvelox.so` / `libgluten.so` with all dependencies linked in statically through
vcpkg, so the result runs on most Linux distributions (see
[static link](velox-backend-build-in-docker.md)).

| Tag | Base | Compiler | `JAVA_HOME` |
|---|---|---|---|
| `vcpkg-almalinux-8-gcc13` | `almalinux:8` (glibc 2.28) | gcc-toolset-13 | JDK 8 |
| `vcpkg-centos-8-gcc13` | `inteldpo/gluten-ci-images:centos-8_gcc13` | GCC 13 | JDK 8 |
| `vcpkg-centos-9` | `quay.io/centos/centos:stream9` | gcc-toolset-12 and 13 | JDK 17 |

All three contain:
- **vcpkg binary cache:** the binary packages for every port in `dev/vcpkg`, built with
  S3/GCS/HDFS/ABFS and tests enabled by `dev/vcpkg/populate-static-image-cache.sh`, in
  `/var/cache/vcpkg`. `VCPKG_BINARY_SOURCES` points at it, so `--enable_vcpkg=ON` builds restore
  dependencies instead of compiling them. Set `VCPKG_BINARY_SOURCES=clear` to disable that.
- **Maven repository:** `/root/.m2`, filled by
  `mvn dependency:go-offline -Pbackends-velox -Piceberg -Pdelta -Pspark-3.5`.
- **CI tools:** `gh` and `jq`. `vcpkg-almalinux-8-gcc13` also has tzdata and Python 3.9 as `python3`.

### Dynamic build and test images (`centos-8-jdk*`, `centos-9-jdk*`)

These build against shared system libraries, and the Spark unit tests run in them.

| Tag | Base | Compiler | `JAVA_HOME` |
|---|---|---|---|
| `centos-8-jdk8` / `-jdk11` / `-jdk17` | `centos:8` (EOL, repos point at `vault.centos.org`) | gcc-toolset-12 | JDK 8 / 11 / 17 |
| `centos-9-jdk8` / `-jdk17` | `quay.io/centos/centos:stream9` | gcc-toolset-12 and 13 | JDK 8 / 17 |

They contain:
- **Arrow:** installed into system paths by `builddeps-veloxbe.sh build_arrow`.
- **Spark test resources:** for Spark 3.4, 3.5, 3.5 with Scala 2.13, 4.0, 4.1 and 4.2, in
  `/opt/shims/spark<version>/spark_home` (from `.github/workflows/util/install-spark-resources.sh`).
  The unit tests point `spark.test.home` there.
- **Release tarballs** in `/opt`: Celeborn 0.5.4 and 0.6.3. The CentOS 8 images also have Uniffle
  0.10.0 and Hadoop 2.10.2.
- **Maven repository:** `/root/.m2` for Spark 3.5, as in the static images.
- **JDK 17** next to the tag's default JDK, for the Spark unit tests, at `/usr/lib/jvm/java-17-openjdk`.
- **PySpark virtualenvs:** one per PySpark version used by the Spark unit tests, at
  `/opt/pyspark/3.4.4` and `/opt/pyspark/3.5.5`. Each has pandas 2.2.3, pyarrow 20.0.0, cython,
  regex and requests, on Python 3.9 (CentOS 8) or 3.11 (CentOS 9). Select one by putting its `bin/`
  first in `PATH`, for example `echo /opt/pyspark/3.5.5/bin >> $GITHUB_PATH`. The system `python3`
  is unchanged.
- **CI tools:** `gh`, `jq`, procps-ng, which, findutils, gzip and tzdata. CentOS 8 also has patchelf;
  CentOS 9 also has `clang-tools-extra` (clang-tidy) and `regex` for `dev/check.py`.

### GPU image (`centos-9-jdk17-cuda13.1-cudf`)

| Tag | Base | Compiler | CUDA |
|---|---|---|---|
| `centos-9-jdk17-cuda13.1-cudf` | `quay.io/centos/centos:stream9` | gcc-toolset-14 | 13.1 (`CUDA_ARCHITECTURES=75`) |

It uses JDK 17 and contains Gluten's native dependencies built with `--enable_gpu=ON`. See
[GPU support](../get-started/VeloxGPU.md).

### Runtime images (`*-runtime`)

The TPC jobs check that the static native library runs on a plain OS, so they must not run in a
build image whose Arrow and other libraries could hide a missing dependency. These images add only
JDKs and CI tools to the OS image, with every JDK installed side by side. Jobs pick one through
`JAVA_HOME`.

| Tag | Base | JDKs (`/usr/lib/jvm/`) | Also contains |
|---|---|---|---|
| `centos-8-runtime` | `centos:8` (repos point at `vault.centos.org`) | `java-1.8.0-openjdk`, `java-11-openjdk`, `java-17-openjdk` | Celeborn 0.5.4 and 0.6.3 tarballs in `/opt` |
| `centos-9-runtime` | `quay.io/centos/centos:stream9` | `java-1.8.0-openjdk`, `java-11-openjdk`, `java-17-openjdk`, `java-21-openjdk` | |
| `ubuntu-22.04-runtime` | `ubuntu:22.04` | `java-{8,11,17,21,25}-openjdk-amd64` | Hadoop 3.3.6 in `/opt/hadoop-3.3.6`, MinIO and `mc` in `/usr/local/bin` |

All three have `gh`, `jq`, wget, procps and tzdata. `install-spark-deps.sh` reuses the
pre-installed Hadoop and MinIO instead of downloading them.

## How the images are published

`docker_image.yml` runs:
- **weekly**, on Sundays at 20:00 UTC;
- **on every push to `main`** that changes `dev/docker/`, `docker_image.yml`,
  `.github/workflows/util/install-spark-deps.sh` or
  `.github/workflows/util/install-spark-resources.sh`.

Each run rebuilds every image from scratch, against the current `main` of `apache/gluten`. The
`vcpkg-*` and `centos-*-jdk*` Dockerfiles clone `GLUTEN_REPO` at `GLUTEN_BRANCH`, which default to
`https://github.com/apache/gluten` and `main`. The GPU Dockerfile always clones `main`.
Multi-arch tags are built on an x86 and an arm64 runner, pushed by digest, and then combined into
one tag by the `merge*` jobs.

Things to keep in mind:
- **Publishing only works from `apache/*`.** It needs the `DOCKERHUB_USER` and `DOCKERHUB_TOKEN`
  secrets, which are not available to forks or pull requests. A pull request that changes a
  Dockerfile therefore does not publish anything; its images appear after it is merged.
- **A failed build leaves the previous image in place.** If either architecture of a multi-arch
  tag fails, its `merge` job does not run and the tag keeps its previous version. Check the tag's
  last-updated date on Docker Hub when a change does not seem to have landed.
- **Tags move.** Every tag is overwritten by each build, so CI always uses the latest image. To
  reproduce an older environment, pull by digest (`apache/gluten@sha256:...`). The Iceberg harness
  pins its images this way (`pin_image` in `.github/workflows/util/iceberg-upstream/baseline.py`).
- **Older tags are no longer rebuilt.** Docker Hub also has release tags (`1.3-*`, `1.4-*`,
  `1.5-*`), `vcpkg-centos-7*`, `vcpkg-centos-8`, `centos-8`, `centos-8-dev`,
  `centos-9-jdk8-cudf`, `centos-9-jdk11`, `centos-9-jdk17-cuda12.9-cudf`, `ubuntu22-m2-cache` and
  `gluten-vcpkg-builder_*`. They are no longer updated. Some are still used by release branches:
  `vcpkg-centos-7` (`branch-1.2` to `branch-1.6`), `vcpkg-centos-7-gcc13` (`branch-1.7`),
  `vcpkg-centos-8` and `centos-9-jdk8-cudf`.

## Building an image locally

Build from the repository root. The Dockerfiles bind-mount helper scripts such as
`dev/docker/install-ci-tools.sh` from the build context with `RUN --mount=type=bind`, which needs
BuildKit (the default in current Docker).

```bash
docker buildx build --load \
  -f dev/docker/Dockerfile.centos9-dynamic-build \
  --build-arg JAVA_VERSION=17 \
  -t gluten-local:centos-9-jdk17 .
```

The mounted helper scripts come from your working tree. The dependencies baked into the image
(Arrow, vcpkg packages, `/root/.m2`) come from the clone made inside the Dockerfile instead. For
the `vcpkg-*` and `centos-*-jdk*` images, pass
`--build-arg GLUTEN_REPO=<repository URL> --build-arg GLUTEN_BRANCH=<branch>` to build them from
another branch.

The dynamic and static build images take one to several hours to build, mostly for Arrow and the
vcpkg ports.
