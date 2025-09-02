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

FROM --platform=$BUILDPLATFORM gradle:8.13-jdk17 AS builder

RUN mkdir /code
WORKDIR /code
ENV GRADLE_USER_HOME=/code/.gradlecache \
   GRADLE_OPTS="-Djdk.lang.Process.launchMechanism=vfork -Dorg.gradle.vfs.watch=false"

COPY ./build.gradle.kts ./gradle.properties ./settings.gradle.kts /code/
COPY ./buildSrc /code/buildSrc

RUN gradle downloadDependencies copyDependencies startScripts

COPY ./src /code/src

RUN gradle jar

FROM eclipse-temurin:17-jre

MAINTAINER Joris Borgdorff <joris@thehyve.nl>, Yatharth Ranjan<yatharth.ranjan@kcl.ac.uk>

LABEL description="RADAR-base output data restructuring"

ENV RADAR_OUTPUT_RESTRUCTURE_OPTS="" \
  JAVA_OPTS="-XX:+UseG1GC -XX:MaxHeapFreeRatio=10 -XX:MinHeapFreeRatio=10"

COPY --from=builder /code/build/third-party/* /usr/lib/
COPY --from=builder /code/build/scripts/* /usr/bin/
COPY --from=builder /code/build/libs/* /usr/lib/

RUN mkdir /output \
  && chown 101:101 /output

VOLUME ["/output"]

USER 101:101

ENTRYPOINT ["radar-output-restructure"]
