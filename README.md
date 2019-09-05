# analyze-dedup

is a python script to analyze dedup usage in btrfs. It parses a subvolume and it calculates how much disk space is gained by dedup / reflink.
Optionally, it can analyze the dedup percentage of each file in that subvolume.

```
usage: analyze_dedup.py [-h] [-r ROOT] [-o [OUTPUT]] path

positional arguments:
  path                  path of the btrfs filesystem

optional arguments:
  -h, --help            show this help message and exit
  -r ROOT, --root ROOT  current active subvolume to analyze first, default is
                        5
  -o [OUTPUT], --output [OUTPUT]
                        File to write results for all files that are impacted
                        by dedup

```
