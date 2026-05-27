#!/bin/bash
cd /c/Users/ethan/coding_projects/pytrilogy
for n in $(seq -w 1 61); do
  num=$((10#$n))
  if [ -f "local_scripts/v4_compare/query$(printf %02d $num).md" ]; then
    pre=$(git show 1dfd97b5:local_scripts/v4_compare/query$(printf %02d $num).md 2>/dev/null | grep -m1 '^\*\*Status:' | sed 's/.*`\(.*\)`.*/\1/')
    post=$(grep -m1 '^\*\*Status:' "local_scripts/v4_compare/query$(printf %02d $num).md" | sed 's/.*`\(.*\)`.*/\1/')
    if [ "$pre" != "$post" ]; then
      echo "q$(printf %02d $num): $pre -> $post"
    fi
  fi
done
