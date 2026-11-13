"""Generate the toy knowledge graph: three snapshots of a small evolving KG.

    python scripts/make_toy_kg.py --out data/toy/raw

Writes s0.nt, s1.nt, s2.nt (about 7,000 triples each). The toy KG is
used to test a pipeline end to end in seconds; it has typed and untyped
entities, optional properties, multi-valued properties, literals with
datatypes and language tags, and type changes between snapshots.
"""
import argparse
import os
import random

EX = "http://example.org/resource/"
ON = "http://example.org/ontology/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD = "http://www.w3.org/2001/XMLSchema#"


def lit(v, dt=None, lang=None):
    if lang:
        return f'"{v}"@{lang}'
    if dt:
        return f'"{v}"^^<{XSD}{dt}>'
    return f'"{v}"'


def base_graph(rng, n_people=900, n_cities=150, n_countries=20, n_orgs=120, n_films=200):
    ents = {}   # iri -> set of classes
    trip = set()

    def add(s, p, o):
        trip.add((s, p, o))

    countries = [f"{EX}Country_{i}" for i in range(n_countries)]
    cities = [f"{EX}City_{i}" for i in range(n_cities)]
    orgs = [f"{EX}Org_{i}" for i in range(n_orgs)]
    people = [f"{EX}Person_{i}" for i in range(n_people)]
    films = [f"{EX}Film_{i}" for i in range(n_films)]
    for c in countries:
        ents[c] = {ON + "Country", ON + "Place"}
        add(c, ON + "name", lit(c.rsplit("_", 1)[1], lang="en"))
    for c in cities:
        ents[c] = {ON + "City", ON + "Place"}
        add(c, ON + "name", lit("City " + c.rsplit("_", 1)[1], lang="en"))
        add(c, ON + "country", f"<{rng.choice(countries)}>")
        if rng.random() < 0.8:
            add(c, ON + "population", lit(rng.randint(1000, 5_000_000), "integer"))
    for o in orgs:
        ents[o] = {ON + ("University" if rng.random() < 0.3 else "Company"), ON + "Organisation"}
        add(o, ON + "name", lit("Org " + o.rsplit("_", 1)[1]))
        add(o, ON + "location", f"<{rng.choice(cities)}>")
    for p in people:
        # 10 % of people are untyped, as in DBpedia where types are missing
        ents[p] = set() if rng.random() < 0.10 else {ON + "Person"}
        if ents[p] and rng.random() < 0.25:
            ents[p].add(ON + "Scientist")
        add(p, ON + "name", lit("Person " + p.rsplit("_", 1)[1], lang="en"))
        add(p, ON + "birthPlace", f"<{rng.choice(cities)}>")
        if rng.random() < 0.7:
            add(p, ON + "birthDate", lit(f"19{rng.randint(10, 99)}-0{rng.randint(1, 9)}-1{rng.randint(0, 9)}", "date"))
        for _ in range(rng.choice([0, 1, 1, 2])):
            add(p, ON + "employer", f"<{rng.choice(orgs)}>")
    for f in films:
        ents[f] = {ON + "Film", ON + "Work"}
        add(f, ON + "title", lit("Film " + f.rsplit("_", 1)[1], lang="en"))
        add(f, ON + "director", f"<{rng.choice(people)}>")
        for _ in range(rng.randint(1, 4)):
            add(f, ON + "starring", f"<{rng.choice(people)}>")
        if rng.random() < 0.5:
            add(f, ON + "runtime", lit(f"{rng.randint(60, 200)}.0", "double"))
    return ents, trip, people, films, cities, orgs


def evolve(rng, ents, trip, people, films, cities, orgs, step):
    trip = set(trip)
    ents = {k: set(v) for k, v in ents.items()}
    # 1. delete about 4 % of the non-type triples
    for t in rng.sample(sorted(trip), k=len(trip) // 25):
        trip.discard(t)
    # 2. new people and films
    new_people = [f"{EX}Person_new{step}_{i}" for i in range(60)]
    for p in new_people:
        ents[p] = {ON + "Person"} if rng.random() < 0.8 else set()
        trip.add((p, ON + "name", lit("New person", lang="en")))
        trip.add((p, ON + "birthPlace", f"<{rng.choice(cities)}>"))
        # a new optional property appears in later snapshots
        if rng.random() < 0.6:
            trip.add((p, ON + "almaMater", f"<{rng.choice(orgs)}>"))
    for i in range(20):
        f = f"{EX}Film_new{step}_{i}"
        ents[f] = {ON + "Film", ON + "Work"}
        trip.add((f, ON + "title", lit("New film", lang="en")))
        trip.add((f, ON + "director", f"<{rng.choice(people + new_people)}>"))
    # 3. type changes: some untyped people get typed, some people become Scientists
    for p in rng.sample(people, k=40):
        if not ents[p]:
            ents[p] = {ON + "Person"}
        elif ON + "Scientist" not in ents[p]:
            ents[p].add(ON + "Scientist")
    # 4. extra values for existing entities
    for p in rng.sample(people, k=80):
        trip.add((p, ON + "employer", f"<{rng.choice(orgs)}>"))
    return ents, trip, people + new_people, films, cities, orgs


def write(path, ents, trip):
    lines = set()
    for s, p, o in trip:
        lines.add(f"<{s}> <{p}> {o} .")
    for s, cls in ents.items():
        for c in cls:
            lines.add(f"<{s}> <{RDF_TYPE}> <{c}> .")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(lines)) + "\n")
    return len(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/toy/raw")
    ap.add_argument("--seed", type=int, default=562)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rng = random.Random(a.seed)
    state = base_graph(rng)
    for k in range(3):
        if k > 0:
            state = evolve(rng, *state, step=k)
        n = write(os.path.join(a.out, f"s{k}.nt"), state[0], state[1])
        print(f"s{k}.nt: {n} triples")


if __name__ == "__main__":
    main()
