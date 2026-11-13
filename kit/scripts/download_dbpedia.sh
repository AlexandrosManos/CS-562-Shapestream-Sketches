#!/usr/bin/env bash
# Download three English DBpedia snapshots from the DBpedia Databus (CC BY 3.0).
#   bash scripts/download_dbpedia.sh [target_dir]
# About 0.4 GB per version compressed (1.2 GB in total). Spark reads the .bz2 files directly.
# Versions: 2022.03.01, 2022.09.01, 2022.12.01 (2022.12.01 is the last public release).
# The whole list of versions: https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects
set -euo pipefail
TARGET=${1:-data/dbpedia/raw}
BASE=https://downloads.dbpedia.org/repo/dbpedia/mappings
VERSIONS="2022.03.01 2022.09.01 2022.12.01"
for v in $VERSIONS; do
  mkdir -p "$TARGET/$v"
  for f in mappingbased-objects/$v/mappingbased-objects_lang=en.ttl.bz2 \
           mappingbased-literals/$v/mappingbased-literals_lang=en.ttl.bz2 \
           instance-types/$v/instance-types_lang=en_specific.ttl.bz2; do
    out="$TARGET/$v/$(basename "$f")"
    if [ -s "$out" ]; then echo "have $out"; continue; fi
    echo "get $BASE/$f"
    curl -fL --retry 3 -o "$out" "$BASE/$f"
  done
done
echo "done. Next:"
echo "python -m shapestream.prepare_snapshots --out data/dbpedia --sample 0.05 --driver-memory 6g \\"
for i in 0 1 2; do v=$(echo $VERSIONS | cut -d' ' -f$((i+1))); echo "   --snap $i \"$TARGET/$v/*\" \\"; done
