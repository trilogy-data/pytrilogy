"""pytest plugin: with $KS_NOPFB set, `_preserved_final_branch` never fires,
so an atom hosted on a side branch is not restated at FINAL. Run the
`ks_pfb.py` worklist under it to see which tests the restatement is
load-bearing on."""

import os

from trilogy.core.processing.v4_helper import condition_placement

if os.environ.get("KS_NOPFB"):
    condition_placement._preserved_final_branch = lambda *args, **kwargs: False
