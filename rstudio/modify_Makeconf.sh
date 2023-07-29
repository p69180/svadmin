#!/bin/bash
set -eu

original="$1"
backup=$(mktemp "${original}_backup_XXXXXX")
tmp=$(mktemp "${original}_tmp_XXXXXX")
chmod --reference "$original" "$backup"
chmod --reference "$original" "$tmp"

awk \
    -v pat="([[:blank:]])(x86[^[:blank:]]+)" \
    -v bin="${CONDA_PREFIX}/bin" \
    '{print gensub(pat, "\\1" bin "/" "\\2", "g", $0)}' $original > $tmp

mv "$original" "$backup"
mv "$tmp" "$original"
