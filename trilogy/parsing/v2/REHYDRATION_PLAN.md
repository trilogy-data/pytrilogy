
  Problem
                                                  
  _rehydrate_unknown_lineages is a post-hoc fixup loop copied directly from v1's
  run_second_parse_pass (line 502-513 of parse_engine.py). In v1, it's justified
  because v1 literally re-transforms the entire token tree a second time           (reparsed = self.transform(self.tokens[self.token_address]) at line 493) — the
  rehydration loop is just cleanup for datatype inference chains after that        second full pass. We copied the cleanup without copying the re-parse, so we're 
  relying on a bandaid without the surgery it was designed to follow.            
                                                                                 
  The v2 architecture already has the right answer — the phased system described
  in README.md:

  1. COLLECT_SYMBOLS — shallow declaration pass
  2. BIND — resolve names
  3. HYDRATE — build semantic objects
  4. VALIDATE — check invariants
  5. COMMIT — apply to environment

  Today, COLLECT_SYMBOLS and BIND are no-ops for every plan. All work happens in 
  HYDRATE. That means when select_transform for item B calls arbitrary_to_concept
   with a Function referencing concept A (defined by an earlier transform in the 
  same select), concept A isn't in the environment yet — it gets created as      
  UndefinedConceptFull with datatype=UNKNOWN, purpose=UNKNOWN, no grain, no keys.
   The resulting concept B has broken lineage, wrong grain, wrong keys.
  _rehydrate_unknown_lineages patches the datatype but not the
  grain/keys/purpose.

  Root Cause

  The issue is not that we need a post-pass to fix datatypes. The issue is that  
  concept names aren't registered in the environment early enough. By the time   
  HYDRATE runs, every concept name referenced in any expression should already be
   resolvable — at minimum to a stub with the right address so it doesn't become 
  UndefinedConceptFull.

  Proposed Fix

  Step 1: Use COLLECT_SYMBOLS in SelectStatementPlan

  SelectStatementPlan.collect_symbols should walk the syntax tree shallowly to   
  find all SELECT_TRANSFORM nodes and extract their output names. For each one,  
  register a stub Concept in the environment — just address + DataType.UNKNOWN + 
  ConceptSource.SELECT. This is exactly what the README describes: "Register     
  declarations that other statements may refer to."

  # In SelectStatementPlan
  def collect_symbols(self, hydrator: NativeHydrator) -> None:
      # Walk syntax to find SELECT_TRANSFORM children → extract output name      
      # Register stub concept for each: environment.add_concept(stub)

  This means by the time HYDRATE runs, environment.concepts["local.itemrevenue"] 
  returns the stub Concept (not UndefinedConceptFull), so arbitrary_to_concept → 
  function_to_concept can at least find a real concept entry with a valid        
  address.

  Step 2: Use VALIDATE in SelectStatementPlan to fix datatypes

  After HYDRATE has built all the full concepts with proper lineage, the stubs   
  still have DataType.UNKNOWN. SelectStatementPlan.validate should iterate the   
  select's transform concepts and resolve any remaining UNKNOWN datatypes by     
  walking their lineage (the same logic as rehydrate_concept_lineage, but scoped 
  to this statement's concepts, not a global loop).

  # In SelectStatementPlan
  def validate(self, hydrator: NativeHydrator) -> None:
      # For each transform concept in self.output:
      #   if concept.datatype == UNKNOWN and concept.lineage:
      #     rehydrate from lineage (now all args are real concepts)

  Step 3: Do the same for ConceptStatementPlan, GenericStatementPlan, etc.       

  Any plan that creates concepts whose lineage references other concepts in the  
  same parse needs the same pattern: register names early in COLLECT_SYMBOLS, fix
   up datatypes in VALIDATE.

  Key candidates:
  - ConceptStatementPlan — auto concepts referencing earlier concepts
  - GenericStatementPlan — persist, rowset, merge statements that create derived 
  concepts
  - Multi-select — align/derive concepts referencing select outputs

  Step 4: Remove _rehydrate_unknown_lineages and
  rehydrate_lineage/rehydrate_concept_lineage

  Once every plan handles its own datatype resolution in VALIDATE, the global    
  post-pass is dead code. Remove it along with
  self.environment.concepts.undefined = {} (stubs registered via COLLECT_SYMBOLS 
  are real concepts, not undefined entries, so there's nothing to clear).        

  Step 5: Remove fail_on_missing toggle

  This is the long-term goal. Once COLLECT_SYMBOLS pre-registers all concept     
  names before HYDRATE runs, there should be no need for fail_on_missing=False — 
  every referenced concept already exists. If a concept truly doesn't exist,     
  that's a real parse error, not a forward-reference. This eliminates the entire 
  UndefinedConceptFull codepath from v2.

  This step might be deferred if imports introduce ordering complexity, but it   
  should be the explicit target.

  What this fixes

  - q12/q20: Transform concepts referenced before definition → stubs exist from  
  COLLECT_SYMBOLS, so function_to_concept gets a real Concept with valid address,
   proper grain/keys computed in VALIDATE after datatypes resolve.
  - q22: Multi-select grain mismatch → same pattern, align/derive concept names  
  pre-registered.
  - All future forward-reference issues: The pattern is structural, not
  per-bug-report.

  Scope of changes

  ┌──────────────────┬────────────────────────────────────────────────────────┐  
  │       File       │                         Change                         │  
  ├──────────────────┼────────────────────────────────────────────────────────┤  
  │                  │ Add COLLECT_SYMBOLS/VALIDATE to SelectStatementPlan;   │  
  │ hydration.py     │ remove _rehydrate_unknown_lineages, rehydrate_lineage, │  
  │                  │  rehydrate_concept_lineage; remove undefined = {}      │  
  │                  │ cleanup                                                │  
  ├──────────────────┼────────────────────────────────────────────────────────┤  
  │ select_rules.py  │ Possibly extract a helper to walk syntax for transform │  
  │                  │  output names (or this lives in hydration.py)          │  
  ├──────────────────┼────────────────────────────────────────────────────────┤  
  │ hydration.py     │ Same pattern for other plans as needed                 │  
  ├──────────────────┼────────────────────────────────────────────────────────┤  
  │ rules_context.py │ No changes                                             │  
  ├──────────────────┼────────────────────────────────────────────────────────┤  
  │ statements.py    │ No changes                                             │  
  └──────────────────┴────────────────────────────────────────────────────────┘  

  Open question

  The rehydrate_concept_lineage function does two things: (1) re-creates Function
   objects via function_factory.create_function to recompute output_datatype, and
   (2) sets concept.datatype from lineage.output_datatype. For step 2 (VALIDATE),
   we need this same logic but scoped per-statement. Should this be a utility    
  function that VALIDATE calls, or should VALIDATE re-run arbitrary_to_concept   
  entirely (which would also recompute grain/keys/purpose)? Re-running
  arbitrary_to_concept is cleaner — the stub gets fully replaced — but it        
  requires the transform's expression + output name to still be accessible at    
  VALIDATE time, meaning the plan needs to stash intermediate state from HYDRATE.