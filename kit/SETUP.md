# Project A: set-up (done by each team, first week)

Everything below is done by the team. Plan one afternoon; most of the time is waiting for downloads and Spark jobs.
At the end, commit `SETUP_REPORT.txt` to your repository (deadline: Monday 9 November).

## What you need

| Item | Minimum | Notes |
|---|---|---|
| Laptop | 16 GB RAM, 4 cores, 15 GB free disk | one team member's machine is enough for the data preparation |
| Course environment | `04_Environment` (Docker: Spark 4.2, Java 17, JupyterLab, Kafka) | as in the assignments |
| Network | about 1.2 GB of downloads | DBpedia Databus |

Keep `data/` outside cloud-synced folders (OneDrive, Dropbox, iCloud): Spark writes many small files.
`data/` is in `.gitignore`; never commit it.

## Steps

1. **Repository.** Create the team repository and copy this `kit` folder into it. Put the repository inside the
   course folder that `04_Environment/docker-compose.yml` mounts (so the container sees it), on a local disk.
2. **Environment.** `cd 04_Environment && docker compose up -d`, open JupyterLab (http://localhost:8888, token `hy562`)
   and a terminal in it. Then, in the kit folder: `pip install -r requirements.txt`.
3. **Toy data** (already in `data/toy`; 2 minutes to rebuild):
   ```bash
   python scripts/check_setup.py
   ```
   Every line except possibly Kafka must say OK. Run `ProjectA_starter.ipynb` once from top to bottom.
4. **DBpedia** (about 1 hour in total):
   ```bash
   bash scripts/download_dbpedia.sh data/dbpedia/raw
   python -m shapestream.prepare_snapshots --out data/dbpedia --sample 0.05 --driver-memory 6g \
       --snap 0 "data/dbpedia/raw/2022.03.01/*" --snap 1 "data/dbpedia/raw/2022.09.01/*" --snap 2 "data/dbpedia/raw/2022.12.01/*"
   python -m shapestream.make_stream --data data/dbpedia --driver-memory 6g
   python -m shapestream.make_stream --data data/dbpedia --mask-types 0.3 --name masked30 --driver-memory 6g
   python -m shapestream.gold_shapes --data data/dbpedia --driver-memory 6g
   ```
   `--driver-memory 6g` assumes Docker may use at least 8 GB (Docker Desktop: Settings, Resources).
   If a step runs out of memory, use `--sample 0.02` and say so in the report. Share the resulting
   `data/dbpedia` folder inside the team (zip it), so that the other members do not repeat the work.
   All teams of Project A use `--sample 0.05` and the default seed, so everybody has the same data.
5. **Kafka.** With the course docker-compose running:
   ```bash
   python -m shapestream.replay_to_kafka --stream data/toy/streams/full --topic kg-changes
   ```
   then run the Kafka cell of the starter notebook and check that events arrive.
6. **Report.**
   ```bash
   python scripts/check_setup.py --data data/dbpedia
   git add SETUP_REPORT.txt && git commit -m "set-up report"
   ```

## Larger data (optional)

With more RAM, build a second set with `--sample 0.2 --out data/dbpedia20` for the scalability plots. The
LUBM generator (`scripts/make_lubm.sh`) gives insert-only growth; try it with one university first.

## When something fails

- `java.lang.OutOfMemoryError`: raise Docker's memory limit or lower `--sample`.
- Downloads stop: `download_dbpedia.sh` skips files that are already complete, so run it again.
- Kafka not reachable from the container: the broker is `kafka:9092` inside the compose network and
  `localhost:29092` from the host.
- Ask the other Project A teams in the shared channel first: everybody runs the same steps.
