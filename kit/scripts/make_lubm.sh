#!/usr/bin/env bash
# Synthetic insert-heavy streams from the LUBM generator (UBA 1.7, Lehigh University), for scalability runs.
#   bash scripts/make_lubm.sh [universities_per_snapshot] [snapshots]
# Snapshot k contains universities 0 .. (k+1)*N-1, so every epoch adds N universities.
# UBA writes RDF/XML (.owl) files; scripts/rdfxml_to_nt.py converts them to N-Triples.
# Needs Java (present in the course image). Not yet tested on the course image: try it with N=1 first.
set -euo pipefail
N=${1:-5}; K=${2:-3}
DIR=data/lubm; mkdir -p $DIR/raw $DIR/uba
cd $DIR/uba
[ -f uba1.7.zip ] || curl -fL -o uba1.7.zip https://swat.cse.lehigh.edu/projects/lubm/uba1.7.zip
[ -d classes ] || unzip -q -o uba1.7.zip
for ((k=0; k<K; k++)); do
  mkdir -p ../gen/u$k && cd ../gen/u$k
  java -cp ../../uba/classes edu.lehigh.swat.bench.uba.Generator -univ $N -index $((k*N)) -seed 0 \
       -onto http://swat.cse.lehigh.edu/onto/univ-bench.owl
  cd ../../uba
done
cd ../../..
for ((k=0; k<K; k++)); do
  # snapshot k = all universities generated so far
  python scripts/rdfxml_to_nt.py --out $DIR/raw/s$k.nt $(for ((j=0; j<=k; j++)); do echo "$DIR/gen/u$j"; done)
done
echo "next: python -m shapestream.prepare_snapshots --out $DIR --snap 0 $DIR/raw/s0.nt --snap 1 $DIR/raw/s1.nt --snap 2 $DIR/raw/s2.nt"
