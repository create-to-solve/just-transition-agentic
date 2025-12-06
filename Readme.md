# Just Transition Agentic
## Agent-driven, schema-validated pipeline for UK local-authority transition intelligence

Version: v3 (2025)

This repository implements the third-generation architecture of the Just Transition Intelligence System (JTIS).  
It is designed as a clean, reproducible, agentic framework for analysing the structural conditions of UK local-authority transition readiness.

### Core features

- **Agent-driven ingestion:**  
  Data loading governed by a dataset registry and validation schemas.

- **Schema-validated datasets:**  
  Each raw dataset has formal column expectations, year ranges, and numerical rules.

- **Pluggable pipeline stages:**  
  Clear separation for ingestion → harmonisation → indicators → composite scoring → profiles.

- **Diagnostics first:**  
  The `ScoutAgent` validates raw datasets before any processing.

### Current status (v3 baseline)

- Dataset registry established  
- Validation schemas implemented  
- ScoutAgent operational  
- Repository structured for modular expansion  
- No legacy code from v1/v2 retained  

### Next development steps

1. Implement ingestion modules for DESNZ, DfT, ONS, IMD.  
2. Build harmonisation functions to compute LAD–year long tables.  
3. Implement indicators (emissions, transport, population, deprivation).  
4. Implement JTI composite scoring.  
5. Add LAD profile generation.  
6. Add agentic Composer/Interpreter modules for autonomous runs.  
7. Integrate a lightweight dashboard (optional).

This repo is intentionally minimal and clean for clarity, maintainability, and future automation.
