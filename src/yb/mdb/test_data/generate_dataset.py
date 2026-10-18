#!/usr/bin/env python3
"""
Knowledge Graph Dataset Generator
Generates ~300 nodes, ~100 edges, ~10 relation types for SPARQL testing.
"""

import random
import os

random.seed(42)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

PREFIXES = {
    "ex": "http://example.org/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

RELATION_TYPES = {
    "worksFor": "http://example.org/worksFor",
    "locatedIn": "http://example.org/locatedIn",
    "develops": "http://example.org/develops",
    "uses": "http://example.org/uses",
    "manages": "http://example.org/manages",
    "knows": "http://example.org/knows",
    "funds": "http://example.org/funds",
    "collaboratesWith": "http://example.org/collaboratesWith",
    "foundedBy": "http://example.org/foundedBy",
    "partOf": "http://example.org/partOf",
}

companies = [
    "AcmeCorp", "TechNova", "DataSphere", "CloudPeak", "QuantumLeap",
    "BioGenesis", "CyberMatrix", "NovaSystems", "AlphaDyne", "PrimeWave",
    "SynthTech", "OrbitalSolutions", "NexGenRobotics", "DeepMindLabs",
    "FusionWare", "AtlasDigital", "PinnacleAI", "StreamLogic", "ZenithData",
    "CoreInnovations"
]

people = [
    "AliceChen", "BobZhang", "CarolWang", "DavidLi", "EvaLiu",
    "FrankZhao", "GraceWu", "HenryXu", "IrisSun", "JackYang",
    "KarenZhou", "LeoMa", "MiaHuang", "NickLin", "OliviaHe",
    "PeterGao", "QuinnTang", "RachelSong", "SamDeng", "TinaCheng",
    "UmaJoshi", "VictorChiu", "WendyPeng", "XanderFu", "YukiTanaka",
    "ZaraKhan", "AidenShaw", "BellaRao", "CarlosMendez", "DianaLee",
    "EthanChoi", "FionaPark", "GeorgeNg", "HannahJiang", "IanFeng",
    "JuliaTsai", "KevinLu", "LenaZhu", "MarcusWong", "NinaZheng"
]

products = [
    "AnalyticsPro", "DataVault", "CloudSync", "SecureShield", "AIMaster",
    "BioTracker", "RoboAssist", "QuantumSim", "FusionEngine", "CodeWizard",
    "NetGuard", "DeepSearch", "StreamFlow", "PixelForge", "CryptoLock",
    "HealthBot", "EcoTrack", "SwiftDB", "LogicGate", "ZenPlatform"
]

technologies = [
    "Python", "Cpp", "Rust", "Kubernetes", "PostgreSQL",
    "TensorFlow", "React", "Kafka", "Spark", "MongoDB",
    "YugabyteDB", "Docker", "GraphQL", "Redis", "Elasticsearch",
    "Terraform", "Ansible", "Prometheus", "Jenkins", "Nginx"
]

departments = [
    "Engineering", "Research", "Marketing", "Sales", "Finance",
    "HR", "Operations", "Legal", "Product", "Design"
]

cities = [
    "Beijing", "Shanghai", "Shenzhen", "SanFrancisco", "NewYork",
    "London", "Tokyo", "Singapore", "Berlin", "Bangalore",
    "Toronto", "Sydney", "Seoul", "Paris", "Dubai"
]

projects = [
    "ProjectPhoenix", "ProjectTitan", "ProjectAtlas", "ProjectNova",
    "ProjectZenith", "ProjectOrion", "ProjectVoyager", "ProjectMercury",
    "ProjectApollo", "ProjectHermes", "ProjectAurora", "ProjectHelios",
    "ProjectEclipse", "ProjectComet", "ProjectPulsar"
]


def uri(prefix, name):
    return "<{}{}>".format(PREFIXES[prefix], name)


def literal(value, datatype=None):
    if datatype:
        return '"{}"^^{}<{}>'.format(value, datatype, PREFIXES["xsd"])
    return '"{}"'.format(value)


def triple(s, p, o):
    return "{} {} {} .\n".format(s, p, o)


def generate():
    triples = []

    for company in companies:
        triples.append(triple(
            uri("ex", company),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Company")
        ))

    for person in people:
        triples.append(triple(
            uri("ex", person),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Person")
        ))

    for product in products:
        triples.append(triple(
            uri("ex", product),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Product")
        ))

    for tech in technologies:
        triples.append(triple(
            uri("ex", tech),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Technology")
        ))

    for dept in departments:
        triples.append(triple(
            uri("ex", dept),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Department")
        ))

    for city in cities:
        triples.append(triple(
            uri("ex", city),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "City")
        ))

    for proj in projects:
        triples.append(triple(
            uri("ex", proj),
            "<" + PREFIXES["rdf"] + "type>",
            uri("ex", "Project")
        ))

    # -- cities: locatedIn --
    for company in companies:
        city = random.choice(cities)
        triples.append(triple(
            uri("ex", company),
            "<" + RELATION_TYPES["locatedIn"] + ">",
            uri("ex", city)
        ))

    # -- people: worksFor --
    for person in people:
        company = random.choice(companies)
        triples.append(triple(
            uri("ex", person),
            "<" + RELATION_TYPES["worksFor"] + ">",
            uri("ex", company)
        ))

    # -- people: worksFor second job (some people, ~25%) --
    for person in random.sample(people, 10):
        company = random.choice(companies)
        triples.append(triple(
            uri("ex", person),
            "<" + RELATION_TYPES["worksFor"] + ">",
            uri("ex", company)
        ))

    # -- companies: develops products --
    for company in companies[:12]:
        num_products = random.randint(1, 3)
        for _ in range(num_products):
            product = random.choice(products)
            triples.append(triple(
                uri("ex", company),
                "<" + RELATION_TYPES["develops"] + ">",
                uri("ex", product)
            ))

    # -- products: uses technologies --
    for product in products[:15]:
        num_techs = random.randint(1, 4)
        for _ in range(num_techs):
            tech = random.choice(technologies)
            triples.append(triple(
                uri("ex", product),
                "<" + RELATION_TYPES["uses"] + ">",
                uri("ex", tech)
            ))

    # -- people: manages people (~15 edges) --
    for i in range(15):
        manager = random.choice(people[:20])
        subordinate = random.choice(people)
        if manager != subordinate:
            triples.append(triple(
                uri("ex", manager),
                "<" + RELATION_TYPES["manages"] + ">",
                uri("ex", subordinate)
            ))

    # -- people: knows people (~20 edges) --
    for i in range(20):
        p1 = random.choice(people)
        p2 = random.choice(people)
        if p1 != p2:
            triples.append(triple(
                uri("ex", p1),
                "<" + RELATION_TYPES["knows"] + ">",
                uri("ex", p2)
            ))

    # -- companies: funds projects --
    for i in range(12):
        company = random.choice(companies)
        proj = random.choice(projects)
        triples.append(triple(
            uri("ex", company),
            "<" + RELATION_TYPES["funds"] + ">",
            uri("ex", proj)
        ))

    # -- companies: collaboratesWith (~6 edges) --
    for i in range(6):
        c1 = random.choice(companies)
        c2 = random.choice(companies)
        if c1 != c2:
            triples.append(triple(
                uri("ex", c1),
                "<" + RELATION_TYPES["collaboratesWith"] + ">",
                uri("ex", c2)
            ))

    # -- companies: foundedBy person (~8 edges) --
    for i in range(8):
        company = random.choice(companies)
        founder = random.choice(people[:15])
        triples.append(triple(
            uri("ex", company),
            "<" + RELATION_TYPES["foundedBy"] + ">",
            uri("ex", founder)
        ))

    # -- departments: partOf company --
    for company in companies[:8]:
        for dept in random.sample(departments, random.randint(2, 5)):
            triples.append(triple(
                uri("ex", dept),
                "<" + RELATION_TYPES["partOf"] + ">",
                uri("ex", company)
            ))

    # -- projects: uses technologies --
    for proj in projects[:10]:
        num_techs = random.randint(1, 3)
        for _ in range(num_techs):
            tech = random.choice(technologies)
            triples.append(triple(
                uri("ex", proj),
                "<" + RELATION_TYPES["uses"] + ">",
                uri("ex", tech)
            ))

    # -- people: locatedIn (birth place) --
    for person in people[:20]:
        city = random.choice(cities)
        triples.append(triple(
            uri("ex", person),
            "<" + RELATION_TYPES["locatedIn"] + ">",
            uri("ex", city)
        ))

    # labels (rdfs:label)
    for company in companies:
        triples.append(triple(
            uri("ex", company),
            "<" + PREFIXES["rdfs"] + "label>",
            literal(company)
        ))
    for person in people:
        triples.append(triple(
            uri("ex", person),
            "<" + PREFIXES["rdfs"] + "label>",
            literal(person)
        ))
    for product in products:
        triples.append(triple(
            uri("ex", product),
            "<" + PREFIXES["rdfs"] + "label>",
            literal(product)
        ))

    # Deduplicate
    unique = list(dict.fromkeys(triples))

    filepath = os.path.join(OUTPUT_DIR, "knowledge_graph.nt")
    with open(filepath, "w") as f:
        f.writelines(unique)

    # Count nodes and edges
    node_set = set()
    edge_count = 0
    for t in unique:
        parts = t.strip().split()
        if len(parts) >= 3:
            s, p, o = parts[0], parts[1], parts[2]
            node_set.add(s)
            node_set.add(o)
            edge_count += 1

    print(f"Generated: {filepath}")
    print(f"  Total triples: {len(unique)}")
    print(f"  Unique nodes:  {len(node_set)}")
    print(f"  Edges:         {edge_count}")
    print(f"  Relation types: {len(RELATION_TYPES)}")

    return len(unique), len(node_set), edge_count


if __name__ == "__main__":
    generate()
