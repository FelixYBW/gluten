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
build. Pulled images take about twice that on disk.

| Tag | Dockerfile | Arch | Size | Used by |
|---|---|---|---|---|
| [`vcpkg-almalinux-8-gcc13`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-almalinux-8-gcc13) | [`Dockerfile.almalinux8-gcc13-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.almalinux8-gcc13-static-build) | amd64, arm64 | 1.7 GB | Static native builds in `velox_backend_x86`, `delta_spark_ut`, `velox_kafka`, `velox_native_build` (Iceberg), `velox_backend_ansi`, `velox_backend_enhanced`, `velox_nightly`, `velox_backend_cache`, `build_bundle_package` and `build_release`; [release builds](../../../developers/HowToRelease.md); [static build in Docker](velox-backend-build-in-docker.md) |
| [`vcpkg-centos-8-gcc13`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-centos-8-gcc13) | [`Dockerfile.centos8-gcc13-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-gcc13-static-build) | amd64, arm64 | 2.7 GB | Not used by any workflow |
| [`vcpkg-centos-9`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-centos-9) | [`Dockerfile.centos9-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-static-build) | amd64, arm64 | 2.1 GB | Static native build in `velox_backend_arm`; `velox_backend_cache`; the static [dev container](../../../developers/dev-container.md) |
| [`centos-8-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk8) | [`Dockerfile.centos8-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-dynamic-build) | amd64, arm64 | 5.5 GB | Spark 3.4 / 3.5 unit tests and the Uniffle TPC job in `velox_backend_x86`; `velox_backend_enhanced`; `velox_nightly`; [dynamic build in Docker](velox-backend-build-in-docker.md) |
| [`centos-8-jdk11`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk11) | same, `JAVA_VERSION=11` | amd64, arm64 | 5.5 GB | Not used by any workflow |
| [`centos-8-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk17) | same, `JAVA_VERSION=17` | amd64, arm64 | 5.5 GB | Spark 4.0 unit tests and build checks in `velox_backend_x86`; `velox_backend_enhanced`; `flink` |
| [`centos-9-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk8) | [`Dockerfile.centos9-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-dynamic-build) | amd64, arm64 | 5.1 GB | `cpp-test-udf-test` (shared build, C++ tests, clang-tidy) in `velox_backend_x86` and `velox_backend_arm`; `velox_backend_cache`; the default [dev container](../../../developers/dev-container.md) |
| [`centos-9-jdk11`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk11) | same, `JAVA_VERSION=11` | amd64, arm64 | 5.1 GB | Not used by any workflow |
| [`centos-9-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17) | same, `JAVA_VERSION=17` | amd64, arm64 | 5.1 GB | Spark 4.1 / 4.2 unit tests in `velox_backend_x86`; `delta_spark_ut`; `velox_backend_ansi`; the Iceberg test shards (`iceberg_upstream_revision`) |
| [`centos-9-jdk17-cuda12.9-cudf`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17-cuda12.9-cudf) | [`cudf/Dockerfile.centos-9-jdk17-cuda12.9-cudf`](https://github.com/apache/gluten/blob/main/dev/docker/cudf/Dockerfile.centos-9-jdk17-cuda12.9-cudf) | amd64 | 5.1 GB | Not used by any workflow |
| [`centos-9-jdk17-cuda13.1-cudf`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17-cuda13.1-cudf) | [`cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf`](https://github.com/apache/gluten/blob/main/dev/docker/cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf) | amd64 | 13.2 GB | `build-cudf-centos-9` in `velox_backend_x86`; `velox_backend_cache`; [GPU support](../get-started/VeloxGPU.md) |
| [`ubuntu22-m2-cache`](https://hub.docker.com/r/apache/gluten/tags?name=ubuntu22-m2-cache) | [`ubuntu/Dockerfile.ubuntu22-m2-cache`](https://github.com/apache/gluten/blob/main/dev/docker/ubuntu/Dockerfile.ubuntu22-m2-cache) | amd64 | 5.9 GB | Not used by any workflow |

Pull an image with `docker pull apache/gluten:<tag>`, for example:

```bash
docker pull apache/gluten:vcpkg-almalinux-8-gcc13
docker run -it --rm -v "$PWD":/work -w /work apache/gluten:vcpkg-almalinux-8-gcc13 bash
```

Docker picks the matching architecture of a multi-arch tag automatically.

## What the images contain

Each Dockerfile clones Gluten to pre-populate dependencies, then deletes the clone and the ccache,
so the images contain no Gluten source or binaries. The exception is `ubuntu22-m2-cache`, which
keeps the clone in `/opt/gluten` without its build outputs.

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

### Dynamic build and test images (`centos-8-jdk*`, `centos-9-jdk*`)

These build against shared system libraries, and the Spark unit tests run in them.

| Tag | Base | Compiler | `JAVA_HOME` |
|---|---|---|---|
| `centos-8-jdk8` / `-jdk11` / `-jdk17` | `centos:8` (EOL, repos point at `vault.centos.org`) | gcc-toolset-12 | JDK 8 / 11 / 17 |
| `centos-9-jdk8` / `-jdk11` / `-jdk17` | `quay.io/centos/centos:stream9` | gcc-toolset-12 and 13 | JDK 8 / 11 / 17 |

They contain:
- **Arrow:** installed into system paths by `builddeps-veloxbe.sh build_arrow`.
- **Spark test resources:** for Spark 3.4, 3.5, 3.5 with Scala 2.13, 4.0, 4.1 and 4.2, in
  `/opt/shims/spark<version>/spark_home` (from `.github/workflows/util/install-spark-resources.sh`).
  The unit tests point `spark.test.home` there.
- **Release tarballs** in `/opt`: Celeborn 0.5.4 and 0.6.3. The CentOS 8 images also have Uniffle
  0.10.0 and Hadoop 2.10.2.
- **Maven repository:** `/root/.m2` for Spark 3.5, as in the static images.
- **clang-tidy:** the CentOS 9 images include `clang-tools-extra` for the clang-tidy check.

### GPU images (`*-cudf`)

| Tag | Base | Compiler | CUDA |
|---|---|---|---|
| `centos-9-jdk17-cuda12.9-cudf` | `ghcr.io/facebookincubator/velox-dev:adapters` | gcc-toolset-14 | 12.9 (`CUDA_ARCHITECTURES=70`) |
| `centos-9-jdk17-cuda13.1-cudf` | `quay.io/centos/centos:stream9` | gcc-toolset-14 | 13.1 (`CUDA_ARCHITECTURES=75`) |

Both use JDK 17 and contain Gluten's native dependencies built with `--enable_gpu=ON`. See
[GPU support](../get-started/VeloxGPU.md).

### `ubuntu22-m2-cache`

Ubuntu 22.04 with JDK 17, Maven, Arrow, a `/root/.m2` repository filled for Spark 3.4, 3.5, 4.0
and 4.1, and a Gluten clone in `/opt/gluten`.

## How the images are published

`docker_image.yml` runs:
- **weekly**, on Sundays at 20:00 UTC;
- **on every push to `main`** that changes `dev/docker/`, `docker_image.yml`,
  `.github/workflows/util/install-spark-deps.sh` or
  `.github/workflows/util/install-spark-resources.sh`.

Each run rebuilds every image from scratch, against the current `main` of `apache/gluten`. The
`vcpkg-*` and `centos-*-jdk*` Dockerfiles clone `GLUTEN_REPO` at `GLUTEN_BRANCH`, which default to
`https://github.com/apache/gluten` and `main`. The GPU and `ubuntu22-m2-cache` Dockerfiles always
clone `main`. Multi-arch tags are built on an x86 and an arm64
runner, pushed by digest, and then combined into one tag by the `merge*` jobs.

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
  `centos-9-jdk8-cudf` and `gluten-vcpkg-builder_*`. They are kept for old branches and are not
  updated.

## Building an image locally

Build from the repository root, because the Dockerfiles may reference files outside
`dev/docker/`. BuildKit (the default in current Docker) is required.

```bash
docker buildx build --load \
  -f dev/docker/Dockerfile.centos9-dynamic-build \
  --build-arg JAVA_VERSION=17 \
  -t gluten-local:centos-9-jdk17 .
```

The dependencies baked into the image come from the clone made inside the Dockerfile, not from
your working tree. For the `vcpkg-*` and `centos-*-jdk*` images, pass
`--build-arg GLUTEN_REPO=<repository URL> --build-arg GLUTEN_BRANCH=<branch>` to build them from
another branch.

The dynamic and static build images take one to several hours to build, mostly for Arrow and the
vcpkg ports.
