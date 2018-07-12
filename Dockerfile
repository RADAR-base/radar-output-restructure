# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8-alpine AS builder

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS -Dorg.gradle.daemon=false

COPY ./gradle /code/gradle
COPY ./gradlew /code/
RUN ./gradlew --version

COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN ./gradlew downloadDependencies

COPY ./src /code/src
COPY LICENSE README.md /code/

RUN ./gradlew distTar && \
  tar xzf build/distributions/*.tar.gz && \
  rm build/distributions/*.tar.gz

FROM openjdk:8-jre-alpine

MAINTAINER Joris Borgdorff <joris@thehyve.nl>

LABEL description="RADAR-base HDFS data restructuring"

COPY --from=builder /code/radar-hdfs-restructure-*/bin /usr/bin
COPY --from=builder /code/radar-hdfs-restructure-*/share /usr/share
COPY --from=builder /code/radar-hdfs-restructure-*/lib /usr/lib

ENTRYPOINT ["radar-hdfs-restructure"]
