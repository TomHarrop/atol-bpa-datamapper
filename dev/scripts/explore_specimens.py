#!/usr/bin/env python

import jsonlines
import gzip

nodes_file = "dev/nodes.dmp"
names_file = "dev/names.dmp"
grouped_organisms = "test/grouped_by_organism.jsonl.gz"

mydict = {}
with gzip.open(grouped_organisms) as f:
    reader = jsonlines.Reader(f)
    for obj in reader:
        mydict.update(obj)
