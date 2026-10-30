SHELL = /bin/bash

tmpdir := $(shell mktemp -d)
branch := $(shell git rev-parse --abbrev-ref HEAD)

graph: assets/graph.png
changelog: CHANGELOG.md

CHANGELOG.md: .git/index
	gitchangelog > $(tmpdir)/CHANGELOG.md
	apptainer exec docker://davidanson/markdownlint-cli2:v0.13.0 \
	markdownlint-cli2 \
	--fix \
	:$(tmpdir)/CHANGELOG.md || true
	mv $(tmpdir)/CHANGELOG.md ./CHANGELOG.md
	git add CHANGELOG.md
	git commit -m 'changelog !cosmetic'
	git push origin $(branch)

assets/graph.png: dev/workflow/run_mapper.smk
	DM_CONTAINER=0 \
	snakemake \
	--rulegraph \
	--forceall \
	-s dev/workflow/run_mapper.smk \
	| dot -Tpng \
	> assets/graph.png