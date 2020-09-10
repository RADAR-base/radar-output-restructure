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

FROM gradle:6.6.1-jdk11 AS builder

RUN mkdir /code
WORKDIR /code

ENV GRADLE_USER_HOME=/code/.gradlecache

COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN gradle downloadDependencies copyDependencies startScripts

COPY ./src /code/src

RUN gradle jar

FROM openjdk:11-jre-slim

MAINTAINER Joris Borgdorff <joris@thehyve.nl>, Yatharth Ranjan<yatharth.ranjan@kcl.ac.uk>

LABEL description="RADAR-base output data restructuring"

ENV RADAR_OUTPUT_RESTRUCTURE_OPTS=""
ENV JAVA_OPTS="-Djava.security.egd=file:/dev/./urandom -XX:+UseG1GC -XX:MaxHeapFreeRatio=10 -XX:MinHeapFreeRatio=10"

COPY --from=builder /code/build/third-party/* /usr/lib/
COPY --from=builder /code/build/scripts/* /usr/bin/
COPY --from=builder /code/build/libs/* /usr/lib/

ENTRYPOINT ["radar-output-restructure"]
