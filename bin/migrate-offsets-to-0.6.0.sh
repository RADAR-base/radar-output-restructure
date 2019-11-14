#!/bin/sh

set -e
if [ ! -f offsets.csv ]; then
  echo "Can only migrate offsets if the current directory contains offsets.csv"
  exit 1
fi

mkdir -p offsets
TOPICS=$(tail -n+2 offsets.csv  | cut -d , -f 4 | sort -u)
for topic in $TOPICS; do
  target="offsets/$topic.csv"
  echo "Updating $target"
  if [ ! -f "$target" ]; then
    head -n 1 offsets.csv > "$target"
  fi
  grep ",$topic\$" offsets.csv >> "$target"
done
