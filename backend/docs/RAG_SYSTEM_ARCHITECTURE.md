# RASED RAG System Architecture

## Goal

Build a hackathon-winning recommendation engine that converts Algerian insurance portfolio data, seismic concentration signals, and regulatory knowledge into explainable risk guidance.

## Core Mission

The RAG system is the intelligence layer of the platform. It answers questions such as:

- What is the biggest portfolio risk right now?
- Which wilayas or communes are overexposed?
- Where is pricing below catastrophe adequacy?
- Should the insurer increase reinsurance support?
- Which actions should be prioritized first?

## Business Outcome

The RAG layer turns:

- policy-level structured data
- portfolio aggregates
- hotspot analytics
- pricing adequacy diagnostics
- Algerian seismic and insurance knowledge

into:

- executive summaries
- risk insights
- prioritized recommendations
- confidence scores
- explainable evidence

## Recommended Architecture

### High-level flow

1. Ingest structured portfolio data from PostgreSQL.
2. Compute exposure and pricing aggregates.
3. Retrieve relevant regulatory and insurance knowledge.
4. Assemble a portfolio-aware context package.
5. Generate explainable recommendations.
6. Return structured JSON to the frontend and APIs.

### Runtime layers

- `PostgreSQL`: source of truth for policies and aggregates.
- `Geo analytics service`: portfolio KPIs, hotspots, zone summaries, premium adequacy.
- `RAG knowledge base`: regulatory, actuarial, and reinsurance documents.
- `Hybrid retriever`: matches user query plus portfolio context to relevant knowledge chunks.
- `Recommendation engine`: produces answer, insights, and actions.
- `FastAPI RAG endpoints`: expose the system to frontend and analysts.

## Current Implementation Strategy

The current codebase uses an offline-capable hybrid RAG engine so the endpoints work immediately in hackathon conditions without depending on external AI providers.

### Why this is the right first version

- It works with the current backend and imported dataset.
- It remains fast and deterministic for demos.
- It produces structured responses without waiting on third-party API setup.
- It leaves a clear upgrade path to Gemini, GPT, or open-source LLMs later.

## Data Inputs

### Structured database inputs

From `policies`:

- `policy_year`
- `numero_police`
- `type_risque`
- `code_wilaya`
- `wilaya`
- `code_commune`
- `commune`
- `zone_sismique`
- `capital_assure`
- `prime_nette`

### Derived aggregates

- total exposure
- retained exposure
- exposure by zone
- top hotspot communes
- pricing adequacy by zone and risk type
- concentration alerts

### Knowledge inputs

- RPA 99 zoning and structural guidance
- catastrophe pricing guidance by zone
- reinsurance structuring guidance
- concentration management thresholds
- Algerian seismic history

## Hybrid Retrieval Design

### Retrieval objective

Find the most relevant domain knowledge for the user question and the current portfolio context.

### Retrieval signals

- token overlap with user query
- token overlap with portfolio context query
- term frequency in knowledge chunks
- tag matching
- source-specific bonus for RPA 99 and seismic topics

### Why hybrid retrieval

Pure semantic retrieval is ideal later, but hybrid retrieval wins early because it is:

- robust with small knowledge corpora
- easy to explain
- fast to run in memory
- dependency-light for a hackathon build

## Recommendation Pipeline

### Step 1. Build portfolio context

The service collects:

- portfolio KPIs
- top hotspots
- premium adequacy rows
- concentration alerts
- dominant zones and exposures

### Step 2. Build search query

The retriever combines:

- user question
- dominant zone labels
- top hotspot commune
- pricing and reinsurance intent

### Step 3. Retrieve evidence

The knowledge base returns top matching documents with:

- source
- title
- tags
- excerpt
- score

### Step 4. Generate insights

The engine computes:

- dominant seismic zone
- highest hotspot
- largest pricing deficiency
- retained concentration pressure

### Step 5. Generate recommendations

Each recommendation includes:

- priority
- category
- title
- description
- action
- confidence
- explanation
- evidence
- optional RPA reference

## FastAPI Endpoint Design

### `GET /api/v1/rag/health`

Purpose:

- checks RAG service readiness

Returns:

- status
- model loaded
- model provider
- vector db status
- knowledge document count
- last initialization timestamp

### `POST /api/v1/rag/query`

Purpose:

- answers a free-form analyst question

Input:

```json
{
  "query": "What is my biggest risk?",
  "scope": "portfolio",
  "top_k": 4
}
```

Returns:

- answer
- executive summary
- confidence
- recommendations
- context sources
- retrieved documents

### `GET /api/v1/rag/portfolio-analysis`

Purpose:

- returns a high-level AI-ready summary of the portfolio

Returns:

- executive summary
- total exposure
- total policies
- net retention
- top hotspots
- zone breakdown
- concentration alerts

### `GET /api/v1/rag/risk-insights`

Purpose:

- returns the main structured risks detected by the engine

Returns:

- list of insights
- severity
- description
- metric name and value
- explanation
- retrieved documents

### `GET /api/v1/rag/recommendations`

Purpose:

- returns prioritized recommendation items without a user query

Returns:

- executive summary
- recommendations
- confidence
- retrieved documents

### `POST /api/v1/rag/ingest`

Purpose:

- ingests new knowledge documents

Input:

```json
{
  "documents": [
    {
      "title": "New underwriting note",
      "content": "Zone III facultative support should be strengthened.",
      "source": "Internal Guidelines",
      "tags": ["underwriting", "zoneIII"]
    }
  ]
}
```

Returns:

- ingested document count
- total document count
- status

## Folder Structure

```text
backend/
  app/
    api/
      v1/
        endpoints/
          rag.py
    core/
      dependencies.py
    rag/
      __init__.py
      knowledge_base.py
      service.py
    schemas/
      recommendation.py
    services/
      geo_service.py
  docs/
    RAG_SYSTEM_ARCHITECTURE.md
```

## Scalability Roadmap

### Phase 1. Hackathon-ready

- in-memory hybrid retriever
- persisted JSON knowledge store
- deterministic recommendation engine
- PostgreSQL-backed portfolio analytics

### Phase 2. Production-grade retrieval

- swap hybrid in-memory retrieval for Qdrant or Weaviate
- move knowledge persistence from JSON to vector DB
- add embeddings for regulation chunks and internal documents
- add metadata filtering by source, topic, and regulation type

### Phase 3. LLM-assisted generation

- add Gemini 1.5 Flash or GPT as generator
- keep current deterministic engine as fallback
- enforce JSON schema outputs
- stream token output through SSE

### Phase 4. Analyst feedback loop

- thumbs up / thumbs down on recommendations
- recommendation audit log
- query history
- fine-tune retrieval weighting based on analyst corrections

## Feature Engineering Ideas

- retained exposure share by commune
- commune concentration as share of retained portfolio
- zone-weighted exposure score
- pricing adequacy delta
- ratio of premium to catastrophe exposure
- recurrence-aware hotspot severity

## Explainability Design

Every recommendation should cite:

- the measured portfolio signal
- the triggered business rule
- the retrieved document source
- the recommended action

This keeps the platform professional and defensible during a demo.

## Deployment Guidance

### Backend

- FastAPI on Render
- PostgreSQL on Render
- initialize RAG service at startup
- keep knowledge store on ephemeral filesystem for hackathon

### Future production

- move knowledge store to object storage or vector database
- add Redis cache for repeated query responses
- add async job queue for bulk ingestion

## Sprint Plan

### Sprint 1. Data and analytics foundation

- import policy dataset
- expose geo analytics endpoints
- verify hotspots, KPIs, and premium adequacy

### Sprint 2. RAG foundation

- create knowledge base
- build hybrid retriever
- add health and ingest endpoints

### Sprint 3. Recommendation engine

- implement portfolio analysis
- implement risk insights
- implement recommendation scoring and confidence

### Sprint 4. UX and API polish

- natural language query endpoint
- structured evidence payloads
- docs, examples, and frontend integration

### Sprint 5. Production hardening

- vector DB migration
- LLM integration
- caching
- observability

## Hackathon-winning Add-ons

- commune-level risk narrative cards
- underwriting watchlist for top hotspots
- reinsurance strategy explainer
- side-by-side before/after recommendation simulator
- confidence heatmap over the portfolio map

## Final Positioning

This RAG system is designed to behave like an AI catastrophe portfolio analyst:

- it knows the current portfolio
- it knows the concentration profile
- it knows pricing adequacy
- it knows the relevant Algerian seismic guidance
- it translates all of that into professional actions

That is the layer that makes the platform feel intelligent, explainable, and demo-ready.

