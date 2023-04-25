#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage() {
  echo
  echo "Options:"
  echo "  -h     Display help"
  echo "  -u     SonarQube Host URL"
  echo "  -l     SonarQube Login Credentials"
  echo "  -k     SonarQube Project Key"
  echo "  -n     SonarQube Project Name"
  echo "  -m     Additional Maven options"
  echo "  -t     Number of threads (example: 1 or 2C)."
  echo
  echo "Important:"
  echo "  The required parameters for publishing the coverage results to SonarQube are:"
  echo "    - Host URL"
  echo "    - Login Credentials"
  echo "    - Project Key"
  echo
}

execute() {
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
  MAIN_POM="${SCRIPT_DIR}/../../pom.xml"

  echo "Run tests"
  mvn -B -e -f "${MAIN_POM}" clean install -Pcoverage -fn -Dmaven.javadoc.skip=true

  echo "Run sonar analysis"
  # If the required parameters are given, the code coverage results are uploaded to the SonarQube Server
  if [ -n "$SONAR_LOGIN" ] && [ -n "$SONAR_PROJECT_KEY" ] && [ -n "$SONAR_URL" ]; then
    mvn -B -e -f "${MAIN_POM}" -Pcoverage sonar:sonar -Dsonar.host.url="$SONAR_URL" \
      -Dsonar.login="$SONAR_LOGIN" -Dsonar.projectKey="$SONAR_PROJECT_KEY" -Dsonar.projectName="$SONAR_PROJECT_NAME"
  fi
}

while getopts ":u:l:k:n:h" option; do
  case $option in
  u) SONAR_URL=${OPTARG:-} ;;
  l) SONAR_LOGIN=${OPTARG:-} ;;
  k) SONAR_PROJECT_KEY=${OPTARG:-} ;;
  n) SONAR_PROJECT_NAME=${OPTARG:-} ;;
  h) # Display usage
    usage
    exit
    ;;
  \?) # Invalid option
    echo "Error: Invalid option"
    exit
    ;;
  esac
done

# Start code analysis
execute
