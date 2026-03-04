#!/usr/bin/env bash

set -e

# Push SonarQube ondemand binaries (class files) to Artifactory.
# Requirements:
# - Maven settings.xml configured with server id matching ARTIFACTORY_REPO_ID
# - GLUTEN_BRANCH must be set (e.g. passed as Docker build arg)
# - This script is run from anywhere; it will locate Gluten root automatically.
#
# Archive name:
#   <repo_name>_<branch_name>.tar.gz
echo "SONARQUBE PUSH STARTED INSIDE $GLUTEN_ROOT"

if [ -z "$GLUTEN_ROOT" ]; then
  if [ -d "/incubator-gluten" ]; then
    GLUTEN_ROOT="/incubator-gluten"
  else
    GLUTEN_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
  fi
fi

if [ -z "$GLUTEN_BRANCH" ]; then
  echo "Error: GLUTEN_BRANCH is not set; aborting"
  exit 1
fi

ARTIFACTORY_REPO_ID="${ARTIFACTORY_REPO_ID:-my-local-releases}"
# Resolve settings.xml the same way as setup-automated.sh:
# - If MAVEN_SETTINGS is set and points to a file, use that
# - Otherwise fall back to $HOME/.m2/settings.xml
if [ -n "$MAVEN_SETTINGS" ] && [ -f "$MAVEN_SETTINGS" ]; then
  SETTINGS_FILE="$MAVEN_SETTINGS"
else
  SETTINGS_FILE="$HOME/.m2/settings.xml"
fi

echo "Push SonarQube ondemand binaries from GLUTEN_ROOT=${GLUTEN_ROOT}"

if [ ! -f "$SETTINGS_FILE" ]; then
  echo "Warning: settings.xml not found at $SETTINGS_FILE; skipping SonarQube binary push"
  exit 0
fi

ART_USER=$(sed -n "/<id>${ARTIFACTORY_REPO_ID}<\/id>/,/<\/server>/p" "$SETTINGS_FILE" | sed -n 's/^[[:space:]]*<username>\([^<]*\)<\/username>.*/\1/p' | head -1)
ART_TOKEN=$(sed -n "/<id>${ARTIFACTORY_REPO_ID}<\/id>/,/<\/server>/p" "$SETTINGS_FILE" | sed -n 's/^[[:space:]]*<password>\([^<]*\)<\/password>.*/\1/p' | head -1)

if [ -z "$ART_USER" ] || [ -z "$ART_TOKEN" ]; then
  echo "Warning: Could not read username/password for server id ${ARTIFACTORY_REPO_ID} from $SETTINGS_FILE; skipping SonarQube binary push"
  exit 0
fi

echo "Assuming project already built (targets present under ${GLUTEN_ROOT}); only packaging and uploading."

REPO_TOP="$GLUTEN_ROOT"
REPO_NAME="${SONAR_REPO_NAME:-gluten}"

ARCHIVE_BASE="${REPO_NAME}_${GLUTEN_BRANCH}"
STAGING_DIR="${GLUTEN_ROOT}/../${ARCHIVE_BASE}"
ARCHIVE_FILE="${GLUTEN_ROOT}/../${ARCHIVE_BASE}.tar.gz"

SONAR_ARTIFACTORY_BASE="${SONAR_ARTIFACTORY_BASE:-https://na.artifactory.swg-devops.com/artifactory/hyc-cpd-skywalker-team-lakehouse-on-prem-generic-local/sonarqube/ondemand_binaries}"
UPLOAD_URL="${SONAR_ARTIFACTORY_BASE}/${ARCHIVE_BASE}.tar.gz"

echo "Preparing staging at $STAGING_DIR"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"

echo "Collecting module targets under $REPO_TOP"
for d in "$REPO_TOP"/*; do
  [ -d "$d/target" ] || continue
  name=$(basename "$d")
  mkdir -p "$STAGING_DIR/$name"
  cp -r "$d/target" "$STAGING_DIR/$name/"
done

if [ -z "$(find "$STAGING_DIR" -type f 2>/dev/null | head -1)" ]; then
  echo "Warning: No target/ files collected under $REPO_TOP; skipping upload"
  rm -rf "$STAGING_DIR"
  exit 0
fi

echo "Creating archive $ARCHIVE_FILE (name: ${ARCHIVE_BASE}.tar.gz)"
tar -czf "$ARCHIVE_FILE" -C "$(dirname "$STAGING_DIR")" "$ARCHIVE_BASE"

echo "Uploading archive to $UPLOAD_URL"
curl -f -u "${ART_USER}:${ART_TOKEN}" -T "$ARCHIVE_FILE" "$UPLOAD_URL"

if [ "$GLUTEN_BRANCH" = "main" ]; then
  BINARIES_URL="https://na.artifactory.swg-devops.com/artifactory/hyc-cpd-skywalker-team-lakehouse-on-prem-generic-local/sonarqube/binaries/${ARCHIVE_BASE}.tar.gz"
  echo "Branch is 'main' — also uploading to $BINARIES_URL"
  curl -f -u "${ART_USER}:${ART_TOKEN}" -T "$ARCHIVE_FILE" "$BINARIES_URL"
fi

echo "Cleaning up staging and archive"
rm -f "$ARCHIVE_FILE"
rm -rf "$STAGING_DIR"

echo "SonarQube ondemand binaries TAR uploaded successfully"
