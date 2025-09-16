## Fleet Optimizer (Minimal)

This is a slim, single-solver backend with a simple UI and a minimal test suite.

### Backend
- App entrypoint: `backend/main.py` exposes FastAPI with `/health`, `POST /optimize`, and dispatch endpoints:
  - `GET /routes/{truck_id}?date=YYYY-MM-DD` → next 3 stops
  - `POST /routes/{truck_id}/done?date=YYYY-MM-DD` → advance and return next 3
  - `POST /routes/{truck_id}/reopt?date=YYYY-MM-DD` → re-solve remaining for the active truck
- Structure: `backend/api`, `backend/core`, `backend/models`, `backend/config`.
- Run backend:

```bash
poetry install
poetry run uvicorn backend.main:app --reload
```

### Frontend
- Use the `ui-lovable` folder. Run:

```bash
cd ui-lovable
npm i
npm run dev
```

### Tests
- Minimal tests live in `tests/` and `backend/tests/`.

```bash
poetry run pytest -q
```

### Config
- Tunables in `backend/config/settings.yaml`:
  - `dispatch.batch_size` (default 3)
  - `workday.start` for ETA scheduling (default 07:00)
  - `ml_calibration.enabled` (no-op stub by default)

### Notes
- Only the PyVRP-based solver is kept (`backend/core/solver_pyvrp.py`).
- ETAs come directly from the solver’s duration matrix; no clamps or post-fixes.
- Dispatcher slices 3-at-a-time and emits a simple Google Maps URL placeholder.
poetry install
