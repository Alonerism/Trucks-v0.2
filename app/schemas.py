"""
Pydantic schemas for configuration, settings, and API validation.
"""

from datetime import time
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator, AliasChoices
from pydantic_settings import BaseSettings


class TruckConfig(BaseModel):
    """Truck configuration from params.yaml."""
    name: str
    max_weight_lb: float = Field(gt=0)
    bed_len_ft: float = Field(gt=0)
    bed_width_ft: float = Field(gt=0)
    height_limit_ft: Optional[float] = Field(default=None, gt=0)
    large_capable: bool = Field(default=False)
    large_truck: bool = Field(default=False, description="Whether this is a large truck subject to municipal restrictions")


class WorkdayWindow(BaseModel):
    """Time window for workday operations."""
    start: str = Field(pattern=r"^\d{2}:\d{2}$")  # HH:MM format
    end: str = Field(pattern=r"^\d{2}:\d{2}$")    # HH:MM format
    
    @validator('end')
    def end_after_start(cls, v, values):
        """Ensure end time is after start time."""
        if 'start' in values:
            start_time = time.fromisoformat(values['start'])
            end_time = time.fromisoformat(v)
            if end_time <= start_time:
                raise ValueError('End time must be after start time')
        return v


class DepotConfig(BaseModel):
    """Depot configuration."""
    address: str
    workday_window: WorkdayWindow


class FleetConfig(BaseModel):
    """Fleet configuration."""
    trucks: List[TruckConfig]


class ServiceTimesConfig(BaseModel):
    """Service time configuration."""
    by_category: Dict[str, int] = Field(
        description="Service minutes by item category"
    )
    default_location_service_minutes: int = Field(default=5, ge=0)


class ItemCatalogEntry(BaseModel):
    """Item catalog entry from params.yaml."""
    name: str
    category: str = Field(pattern="^(machine|equipment|material|fuel)$")
    weight_lb_per_unit: float = Field(ge=0)
    dims_lwh_ft: Optional[List[float]] = None
    requires_large_truck: bool = Field(default=False)


class ConstraintsConfig(BaseModel):
    """Constraint configuration."""
    big_truck_co_load_threshold_minutes: int = Field(default=15, ge=0)
    default_location_window_start: str = Field(default="07:00")
    default_location_window_end: str = Field(default="16:30")
    volume_checking_enabled: bool = Field(default=False)
    weight_checking_enabled: bool = Field(default=True)


class OvertimeDeferralConfig(BaseModel):
    """Overtime and deferral policy configuration."""
    default_mode: str = Field(default="ask", pattern="^(ask|overtime|defer)$")
    overtime_slack_minutes: int = Field(default=30, ge=0)
    defer_rule: str = Field(default="lowest_priority_first")


class SolverWeightsConfig(BaseModel):
    """Multi-objective weights configuration."""
    drive_minutes: float = Field(default=1.0, gt=0)
    service_minutes: float = Field(default=0.5, gt=0)
    overtime_minutes: float = Field(default=2.0, gt=0)
    max_route_minutes: float = Field(default=0.1, gt=0)
    priority_soft_cost: float = Field(default=0.2, ge=0)


class LocalSearchConfig(BaseModel):
    """Local search improvement configuration."""
    enabled: bool = Field(default=True)
    iterations: int = Field(default=100, ge=0)
    neighborhood: List[str] = Field(default=["relocate", "swap", "two_opt"])
    time_limit_seconds: int = Field(default=10, ge=0)


class TracingConfig(BaseModel):
    """Tracing configuration."""
    enabled: bool = Field(default=False)
    output_dir: str = Field(default="runs")


class SolverConfig(BaseModel):
    """Solver configuration."""
    use_ortools: bool = Field(default=False)
    single_truck_mode: int = Field(default=0, ge=0, le=1)
    trucks_used_penalty: float = Field(default=1000.0, ge=0)
    random_seed: int = Field(default=42)
    # Normalized balance control (0=Optimal/performance, 1=Balanced, 2=Priority)
    balance_slider: Optional[float] = Field(default=1.0, ge=0, le=2, description="0=Optimal,1=Balanced,2=Priority")
    # Multi-objective weights
    weights: Optional[SolverWeightsConfig] = None
    # Legacy weights (for backward compatibility)
    efficiency_weight: float = Field(default=1.0, gt=0)
    priority_weight: float = Field(default=0.1, ge=0)
    overtime_penalty_per_minute: float = Field(default=2.0, ge=0)
    # Local search parameters
    improve: Optional[LocalSearchConfig] = None
    local_search_iterations: int = Field(default=100, ge=1)
    improvement_threshold: float = Field(default=0.01, gt=0)


class GoogleMapsConfig(BaseModel):
    """Google Maps API configuration."""
    segment_max_waypoints: int = Field(default=9, ge=2, le=25)
    avoid: List[str] = Field(default_factory=list)


class GoogleConfig(BaseModel):
    """Google API configuration."""
    traffic_model: str = Field(
        default="BEST_GUESS", 
        pattern="^(BEST_GUESS|OPTIMISTIC|PESSIMISTIC)$"
    )
    departure_time_offset_hours: int = Field(default=0, ge=0, le=24)
    max_retries: int = Field(default=3, ge=1, le=10)
    retry_delay_seconds: float = Field(default=1.0, gt=0)
    rate_limit_requests_per_second: int = Field(default=10, ge=1, le=100)
    maps: GoogleMapsConfig = Field(default_factory=GoogleMapsConfig)


class RoutingProviderConfig(BaseModel):
    """Routing provider configuration."""
    provider: str = Field(default="osrm", pattern="^(here|ors|osrm|straight)$")
    here_api_key: Optional[str] = Field(default=None)
    ors_api_key: Optional[str] = Field(default=None)
    city_rules_file: str = Field(default="./config/city_rules.yaml")


class CityRule(BaseModel):
    """City-specific truck restriction rule."""
    name: str
    polygon: List[List[float]]  # [[lat, lon], [lat, lon], ...]
    restrictions: Dict[str, Any]  # e.g., {"large_truck_entry_before": "08:00"}


class DatabaseConfig(BaseModel):
    """Database configuration."""
    url: str = Field(default="sqlite:///./truck_optimizer.db")
    echo: bool = Field(default=False)


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field(
        default="INFO", 
        pattern="^(DEBUG|INFO|WARNING|ERROR)$"
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


class DevConfig(BaseModel):
    """Development and testing configuration."""
    mock_google_api: bool = Field(default=False)
    cache_geocoding: bool = Field(default=True)


class ProjectConfig(BaseModel):
    """Top-level project configuration."""
    name: str = Field(default="Concrete Truck Optimizer")
    units: str = Field(default="imperial")
    version: str = Field(default="0.1.0")


class AppConfig(BaseModel):
    """Complete application configuration loaded from params.yaml."""
    project: ProjectConfig
    depot: DepotConfig
    fleet: FleetConfig
    service_times: ServiceTimesConfig
    item_catalog: List[ItemCatalogEntry]
    constraints: ConstraintsConfig
    overtime_deferral: OvertimeDeferralConfig
    solver: SolverConfig
    google: GoogleConfig
    routing: Optional[RoutingProviderConfig] = Field(default_factory=lambda: RoutingProviderConfig())
    database: DatabaseConfig
    logging: LoggingConfig
    dev: DevConfig = Field(default_factory=DevConfig)
    tracing: Optional[TracingConfig] = None


class Settings(BaseSettings):
    """Environment-based settings (primarily for secrets)."""
    google_maps_api_key: Optional[str] = Field(default=None, env="GOOGLE_MAPS_API_KEY")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# API Request/Response Schemas
class JobImportRow(BaseModel):
    """Single row from CSV import."""
    location: str
    action: str  # Will be validated as ActionType in service
    items: str  # "item1:qty1; item2:qty2"
    priority: int = Field(default=1)
    notes: str = Field(default="")
    earliest: Optional[str] = Field(default=None)  # ISO format or None
    latest: Optional[str] = Field(default=None)    # ISO format or None
    service_minutes_override: Optional[int] = Field(default=None)


class ImportRequest(BaseModel):
    """Import request for CSV or JSON data."""
    data: List[JobImportRow]
    date: str  # YYYY-MM-DD
    clear_existing: bool = Field(default=False)


class OptimizeRequest(BaseModel):
    """Request parameters for optimization endpoint."""
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    auto: str = Field(default="ask", pattern="^(ask|overtime|defer)$")
    seed: Optional[int] = Field(default=None)
    # Accept both 'single_truck_mode' and 'single_truck' from clients
    single_truck_mode: bool = Field(default=False, validation_alias=AliasChoices("single_truck_mode", "single_truck"))
    solver_strategy: str = Field(default="pyvrp")
    debug: Optional[bool] = Field(default=False, description="Enable verbose debug logging and route.debug payload")
    trace: bool = Field(default=False)
    visualize: bool = Field(default=False)
    output_dir: str = Field(default="runs")
    time_windows_only: bool = Field(default=False, description="Ignore capacity and capability constraints; enforce only time windows")
    # Optional runtime overrides for priority-vs-performance tradeoff
    priority_trade_off: Optional[float] = Field(default=None, ge=0, description="Override for solver.priority.performance_trade_off for this run")
    priority_soft_cost: Optional[float] = Field(default=None, ge=0, description="Override for solver.weights.priority_soft_cost for this run")
    # Normalized balance slider s in [0,2]: 0=favor performance, 1=balanced, 2=favor priority
    balance_slider: Optional[float] = Field(default=None, ge=0, le=2, description="Slider controlling priority vs performance: 0=performance, 1=balanced, 2=priority")


class ConfigUpdateRequest(BaseModel):
    """Request to update configuration parameters."""
    updates: Dict[str, Any] = Field(
        description="Nested dictionary of configuration updates"
    )


class ImportStatsResponse(BaseModel):
    """Response from import operation."""
    locations_created: int
    locations_updated: int
    items_created: int
    jobs_created: int
    total_job_items: int
    geocoding_requests: int
    errors: List[str] = Field(default_factory=list)


class KPIResponse(BaseModel):
    """Key performance indicators for a route plan."""
    total_drive_minutes: float
    total_service_minutes: float
    total_overtime_minutes: float
    trucks_used: int
    jobs_assigned: int
    jobs_unassigned: int
    efficiency_score: float  # Computed metric
    priority_score: float    # Computed metric


class OvertimeDecision(BaseModel):
    """Overtime decision details for a truck."""
    truck_id: int
    truck_name: str
    day_total_minutes: float
    overtime_used_minutes: float
    stops: List[Dict[str, Any]]
    can_fit_with_60min_overtime: bool


class DeferJob(BaseModel):
    """Request to defer a job to next day."""
    job_id: int
    new_priority: Optional[int] = Field(default=None, ge=0, le=3)


class BulkDeferRequest(BaseModel):
    """Request to defer multiple jobs."""
    jobs: List[DeferJob]


class DeferredJob(BaseModel):
    """Deferred job details surfaced when capacity/time fallback defers remaining work."""
    id: int
    priority: int
    reason: str = Field(description="Reason for deferral e.g. deferred_due_to_capacity_time")
    suggested_date: str = Field(description="Next work day (YYYY-MM-DD) the job is suggested to be scheduled")


class HealthResponse(BaseModel):
    """API health check response."""
    status: str
    version: str
    database_connected: bool
    google_api_configured: bool
    timestamp: str
