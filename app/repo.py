"""
Repository layer for database operations.
Provides clean interface for CRUD operations on all entities.
"""

import logging
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from sqlmodel import SQLModel, Session, create_engine, select, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from .models import (
    Truck, Location, Item, Job, JobItem, 
    RouteAssignment, RouteStop, UnassignedJob, ItemMeta,
    ActionType, ItemCategory,
    Driver, DriverDispatchState, DispatchBatchStop, DispatchMessage, DispatchTimeLog
)

logger = logging.getLogger(__name__)
from .schemas import AppConfig


class DatabaseRepository:
    """Database repository for all truck optimizer entities."""
    
    def __init__(self, config: AppConfig):
        """Initialize database connection."""
        self.config = config
        self.engine = create_engine(
            config.database.url,
            echo=config.database.echo
        )
        
    def create_tables(self) -> None:
        """Create all database tables."""
        SQLModel.metadata.create_all(self.engine)
        # Run migrations for soft delete columns if needed
        self._migrate_large_truck_column()
        self._migrate_soft_delete_columns()
        
    def _migrate_large_truck_column(self) -> None:
        """Add large_truck column to existing Truck table if it doesn't exist."""
        try:
            with self.engine.connect() as connection:
                # Check if large_truck column exists
                result = connection.execute(text("PRAGMA table_info(truck)"))
                columns = [row[1] for row in result.fetchall()]
                
                if 'large_truck' not in columns:
                    logger.info("Adding large_truck column to truck table")
                    connection.execute(text("ALTER TABLE truck ADD COLUMN large_truck BOOLEAN DEFAULT 0"))
                    connection.commit()
                    logger.info("Successfully added large_truck column")
                    
        except Exception as e:
            logger.warning(f"Could not migrate large_truck column: {e}")
            # This is non-critical, continue operation
            
    def _migrate_soft_delete_columns(self) -> None:
        """Add soft delete columns to existing Job table if they don't exist."""
        try:
            with self.engine.connect() as connection:
                # Check if soft delete columns exist
                result = connection.execute(text("PRAGMA table_info(job)"))
                columns = [row[1] for row in result.fetchall()]
                
                if 'is_deleted' not in columns:
                    logger.info("Adding is_deleted column to job table")
                    connection.execute(text("ALTER TABLE job ADD COLUMN is_deleted BOOLEAN DEFAULT 0"))
                    connection.commit()
                    logger.info("Successfully added is_deleted column")
                    
                if 'deleted_at' not in columns:
                    logger.info("Adding deleted_at column to job table") 
                    connection.execute(text("ALTER TABLE job ADD COLUMN deleted_at DATETIME"))
                    connection.commit()
                    logger.info("Successfully added deleted_at column")
                    
        except Exception as e:
            logger.warning(f"Could not migrate soft delete columns: {e}")
            # This is non-critical, continue operation
        
    def get_session(self) -> Session:
        """Get database session."""
        # Avoid expiring attributes on commit so returned instances remain usable
        return Session(self.engine, expire_on_commit=False)
    
    # Truck operations
    def get_trucks(self) -> List[Truck]:
        """Get all trucks."""
        with self.get_session() as session:
            return session.exec(select(Truck)).all()

    def get_truck_by_id(self, truck_id: int) -> Optional[Truck]:
        """Get truck by primary key id."""
        with self.get_session() as session:
            return session.get(Truck, truck_id)
    
    def get_truck_by_name(self, name: str) -> Optional[Truck]:
        """Get truck by name."""
        with self.get_session() as session:
            return session.exec(
                select(Truck).where(Truck.name == name)
            ).first()

    def get_trucks_by_name_prefix(self, prefix: str) -> List[Truck]:
        """Get all trucks whose name starts with the given prefix (case-sensitive)."""
        with self.get_session() as session:
            like_pattern = f"{prefix}%"
            return session.exec(
                select(Truck).where(Truck.name.like(like_pattern))
            ).all()
    
    def create_truck(self, truck_data: Dict[str, Any]) -> Truck:
        """Create a new truck."""
        with self.get_session() as session:
            truck = Truck(**truck_data)
            session.add(truck)
            session.commit()
            session.refresh(truck)
            return truck

    def delete_truck(self, truck_id: int) -> bool:
        """Delete a truck (only if it has no route assignments)."""
        with self.get_session() as session:
            truck = session.get(Truck, truck_id)
            if not truck:
                return False
            # Simple constraint: disallow delete if assignments exist
            if truck.route_assignments:
                # Could cascade in future
                return False
            session.delete(truck)
            session.commit()
            return True

    def get_route_assignments_for_truck(self, truck_id: int) -> List[RouteAssignment]:
        """Return all route assignments for a given truck (without stops)."""
        with self.get_session() as session:
            return session.exec(
                select(RouteAssignment).where(RouteAssignment.truck_id == truck_id)
            ).all()

    def delete_truck_and_assignments(self, truck_id: int) -> bool:
        """Delete a truck and all its route assignments and stops.

        Returns True if the truck existed and was deleted, else False.
        """
        with self.get_session() as session:
            truck = session.get(Truck, truck_id)
            if not truck:
                return False

            # Collect assignment ids for this truck
            assignment_ids = session.exec(
                select(RouteAssignment.id).where(RouteAssignment.truck_id == truck_id)
            ).all()

            if assignment_ids:
                # Delete route stops tied to these assignments first
                session.exec(
                    delete(RouteStop).where(RouteStop.route_assignment_id.in_(assignment_ids))
                )
                # Delete the assignments
                session.exec(
                    delete(RouteAssignment).where(RouteAssignment.id.in_(assignment_ids))
                )

            # Finally delete the truck
            session.delete(truck)
            session.commit()
            return True

    # --- Bulk purge helpers (admin/reset) ---
    def purge_routes(self) -> Dict[str, int]:
        """Delete all route assignments, stops, and unassigned job records.

        Returns counts of deleted rows.
        """
        with self.get_session() as session:
            counts: Dict[str, int] = {"route_stops": 0, "route_assignments": 0, "unassigned_jobs": 0}
            # Collect ids for explicit delete ordering
            assignment_ids = session.exec(select(RouteAssignment.id)).all()
            if assignment_ids:
                counts["route_stops"] = session.exec(
                    delete(RouteStop).where(RouteStop.route_assignment_id.in_(assignment_ids))
                ).rowcount or 0
                counts["route_assignments"] = session.exec(
                    delete(RouteAssignment).where(RouteAssignment.id.in_(assignment_ids))
                ).rowcount or 0
            counts["unassigned_jobs"] = session.exec(delete(UnassignedJob)).rowcount or 0
            session.commit()
            return counts

    def purge_jobs(self) -> Dict[str, int]:
        """Hard delete all jobs and related job_items (used only in admin reset)."""
        with self.get_session() as session:
            counts: Dict[str, int] = {"job_items": 0, "jobs": 0}
            job_ids = session.exec(select(Job.id)).all()
            if job_ids:
                counts["job_items"] = session.exec(delete(JobItem).where(JobItem.job_id.in_(job_ids))).rowcount or 0
                counts["jobs"] = session.exec(delete(Job).where(Job.id.in_(job_ids))).rowcount or 0
            session.commit()
            return counts

    def purge_trucks(self) -> Dict[str, int]:
        """Delete all trucks (will cascade manually by first removing their assignments)."""
        # Remove all assignments first
        self.purge_routes()
        with self.get_session() as session:
            count = session.exec(delete(Truck)).rowcount or 0
            session.commit()
            return {"trucks": count}

    def purge_dispatch(self) -> Dict[str, int]:
        """Delete dispatch-related tables (batch stops, states, messages, time logs)."""
        with self.get_session() as session:
            counts: Dict[str, int] = {"batch_stops": 0, "dispatch_states": 0, "messages": 0, "time_logs": 0}
            counts["batch_stops"] = session.exec(delete(DispatchBatchStop)).rowcount or 0
            counts["dispatch_states"] = session.exec(delete(DriverDispatchState)).rowcount or 0
            counts["messages"] = session.exec(delete(DispatchMessage)).rowcount or 0
            counts["time_logs"] = session.exec(delete(DispatchTimeLog)).rowcount or 0
            session.commit()
            return counts

    def full_reset(self, drop_trucks: bool = True, drop_jobs: bool = True) -> Dict[str, Dict[str, int]]:
        """Perform a full system reset.

        Args:
            drop_trucks: also remove all trucks (and their routes) if True
            drop_jobs: remove all jobs + job_items (locations are retained for faster reseed)
        Returns nested counts per category.
        """
        summary: Dict[str, Dict[str, int]] = {}
        # Always clear routes & unassigned first
        summary["routes"] = self.purge_routes()
        summary["dispatch"] = self.purge_dispatch()
        if drop_jobs:
            summary["jobs"] = self.purge_jobs()
        if drop_trucks:
            summary["trucks"] = self.purge_trucks()
        return summary

    def update_truck(self, truck_id: int, updates: Dict[str, Any]) -> Optional[Truck]:
        """Update truck fields and return updated truck."""
        with self.get_session() as session:
            truck = session.get(Truck, truck_id)
            if not truck:
                return None
            for key, value in updates.items():
                if hasattr(truck, key):
                    setattr(truck, key, value)
            session.add(truck)
            session.commit()
            session.refresh(truck)
            return truck
    
    def upsert_trucks(self, trucks_data: List[Dict[str, Any]]) -> List[Truck]:
        """Insert or update trucks from configuration."""
        trucks = []
        for truck_data in trucks_data:
            existing = self.get_truck_by_name(truck_data["name"])
            if existing:
                # Update existing truck
                with self.get_session() as session:
                    for key, value in truck_data.items():
                        if key != "name":  # Don't update the unique key
                            setattr(existing, key, value)
                    session.add(existing)
                    session.commit()
                    session.refresh(existing)
                    trucks.append(existing)
            else:
                # Create new truck
                trucks.append(self.create_truck(truck_data))
        return trucks
    
    # Location operations
    def get_locations(self) -> List[Location]:
        """Get all locations."""
        with self.get_session() as session:
            return session.exec(select(Location)).all()
    
    def get_location_by_name(self, name: str) -> Optional[Location]:
        """Get location by name."""
        with self.get_session() as session:
            return session.exec(
                select(Location).where(Location.name == name)
            ).first()
    
    def create_location(self, location_data: Dict[str, Any]) -> Location:
        """Create a new location."""
        with self.get_session() as session:
            location = Location(**location_data)
            session.add(location)
            session.commit()
            session.refresh(location)
            return location
    
    def update_location_coordinates(
        self, 
        location_id: int, 
        lat: float, 
        lon: float
    ) -> None:
        """Update location coordinates after geocoding."""
        with self.get_session() as session:
            location = session.get(Location, location_id)
            if location:
                location.lat = lat
                location.lon = lon
                session.add(location)
                session.commit()
    
    # Item operations
    def get_items(self) -> List[Item]:
        """Get all items."""
        with self.get_session() as session:
            return session.exec(select(Item)).all()

    def get_item_path(self, item_id: int) -> Optional[List[str]]:
        """Get hierarchical path metadata for an item, if present."""
        with self.get_session() as session:
            meta = session.exec(
                select(ItemMeta).where((ItemMeta.item_id == item_id) & (ItemMeta.key == "path"))
            ).first()
            if not meta or not meta.value_json:
                return None
            try:
                import json
                return json.loads(meta.value_json)
            except Exception:
                return None

    def set_item_path(self, item_id: int, path: Optional[List[str]]) -> None:
        """Set hierarchical path metadata for an item (overwrite existing)."""
        with self.get_session() as session:
            meta = session.exec(
                select(ItemMeta).where((ItemMeta.item_id == item_id) & (ItemMeta.key == "path"))
            ).first()
            import json
            value_json = json.dumps(path) if path is not None else None
            if meta:
                meta.value_json = value_json
                session.add(meta)
            else:
                session.add(ItemMeta(item_id=item_id, key="path", value_json=value_json))
            session.commit()
    
    def get_item_by_name(self, name: str) -> Optional[Item]:
        """Get item by name."""
        with self.get_session() as session:
            return session.exec(
                select(Item).where(Item.name == name)
            ).first()

    def get_item_by_id(self, item_id: int) -> Optional[Item]:
        """Get item by primary key id."""
        with self.get_session() as session:
            return session.get(Item, item_id)
    
    def create_item(self, item_data: Dict[str, Any]) -> Item:
        """Create a new item."""
        with self.get_session() as session:
            # Convert dims_lwh_ft list to JSON string if present
            if "dims_lwh_ft" in item_data and item_data["dims_lwh_ft"]:
                import json
                item_data["dims_lwh_ft"] = json.dumps(item_data["dims_lwh_ft"])
            
            # Extract optional path metadata
            path = item_data.pop("path", None)

            item = Item(**item_data)
            session.add(item)
            session.commit()
            session.refresh(item)
            # Save path metadata if provided
            if path is not None:
                try:
                    session.add(ItemMeta(item_id=item.id, key="path", value_json=(__import__("json").dumps(path))))
                    session.commit()
                except Exception:
                    # Best-effort; ignore meta failures
                    pass
            return item
    
    def upsert_items(self, items_data: List[Dict[str, Any]]) -> List[Item]:
        """Insert or update items from catalog."""
        items = []
        for item_data in items_data:
            existing = self.get_item_by_name(item_data["name"])
            if existing:
                # Update existing item
                with self.get_session() as session:
                    path = item_data.get("path", None)
                    for key, value in item_data.items():
                        if key == "dims_lwh_ft" and value:
                            import json
                            value = json.dumps(value)
                        if key != "name":  # Don't update the unique key
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                    session.add(existing)
                    session.commit()
                    session.refresh(existing)
                    # Update path meta if provided
                    if path is not None:
                        try:
                            meta = session.exec(
                                select(ItemMeta).where((ItemMeta.item_id == existing.id) & (ItemMeta.key == "path"))
                            ).first()
                            import json
                            value_json = json.dumps(path)
                            if meta:
                                meta.value_json = value_json
                                session.add(meta)
                            else:
                                session.add(ItemMeta(item_id=existing.id, key="path", value_json=value_json))
                            session.commit()
                        except Exception:
                            pass
                    items.append(existing)
            else:
                # Create new item
                items.append(self.create_item(item_data))
        return items

    def delete_item(self, item_id: int) -> bool:
        """Delete an item and its metadata if it's not referenced by any job items."""
        with self.get_session() as session:
            # Ensure no JobItem references exist
            ref = session.exec(select(JobItem).where(JobItem.item_id == item_id)).first()
            if ref:
                return False
            # Delete meta
            session.exec(delete(ItemMeta).where(ItemMeta.item_id == item_id))
            # Delete item
            result = session.exec(delete(Item).where(Item.id == item_id))
            session.commit()
            return result.rowcount > 0
    
    # Job operations
    def get_jobs_by_date(self, target_date: str, include_deleted: bool = False) -> List[Job]:
        """Get all jobs for a specific date with eager loading of relationships."""
        with self.get_session() as session:
            from sqlmodel import select
            from sqlalchemy.orm import selectinload
            
            # Eager load job_items and their related items and location
            query = (select(Job)
                .options(
                    selectinload(Job.job_items).selectinload(JobItem.item),
                    selectinload(Job.location)
                )
                .where(Job.date == target_date))
            
            # Exclude soft-deleted jobs by default
            if not include_deleted:
                query = query.where(Job.is_deleted == False)
            
            jobs = session.exec(query).all()
            
            # Trigger loading of all relationships while session is active
            for job in jobs:
                _ = job.job_items  # Force loading
                _ = job.location   # Force loading
                for job_item in job.job_items:
                    _ = job_item.item  # Force loading
            
            return jobs
    
    def get_jobs(self, include_deleted: bool = False) -> List[Job]:
        """Get all jobs with eager loading of relationships."""
        with self.get_session() as session:
            from sqlmodel import select
            from sqlalchemy.orm import selectinload
            
            # Eager load job_items and their related items and location
            query = (select(Job)
                .options(
                    selectinload(Job.job_items).selectinload(JobItem.item),
                    selectinload(Job.location)
                ))
            
            # Exclude soft-deleted jobs by default
            if not include_deleted:
                query = query.where(Job.is_deleted == False)
            
            jobs = session.exec(query).all()
            
            # Trigger loading of all relationships while session is active
            for job in jobs:
                _ = job.job_items  # Force loading
                _ = job.location   # Force loading
                for job_item in job.job_items:
                    _ = job_item.item  # Force loading
            
            return jobs
    
    def create_job(self, job_data: Dict[str, Any]) -> Job:
        """Create a new job."""
        with self.get_session() as session:
            job = Job(**job_data)
            session.add(job)
            session.commit()
            session.refresh(job)
            logger.info(f"Job saved with id={job.id} date={job.date} location_id={job.location_id} priority={job.priority}")
            return job

    def get_job_by_id(self, job_id: int) -> Optional[Job]:
        """Get a specific job by ID with related location and items eagerly loaded."""
        with self.get_session() as session:
            from sqlalchemy.orm import selectinload
            result = session.exec(
                select(Job)
                .options(
                    selectinload(Job.location),
                    selectinload(Job.job_items).selectinload(JobItem.item)
                )
                .where(Job.id == job_id)
            ).first()
            # Force load relationships while session is active
            if result is not None:
                _ = result.location
                for ji in result.job_items:
                    _ = ji.item
            return result
    
    def create_job_item(self, job_item_data: Dict[str, Any]) -> JobItem:
        """Create a job item association."""
        with self.get_session() as session:
            job_item = JobItem(**job_item_data)
            session.add(job_item)
            session.commit()
            session.refresh(job_item)
            return job_item
    
    def delete_jobs_by_date(self, target_date: str) -> int:
        """Delete all jobs for a specific date."""
        with self.get_session() as session:
            # Delete job items first (foreign key constraint)
            job_ids = session.exec(
                select(Job.id).where(Job.date == target_date)
            ).all()
            
            if job_ids:
                session.exec(
                    delete(JobItem).where(JobItem.job_id.in_(job_ids))
                )
                
                deleted_count = session.exec(
                    delete(Job).where(Job.date == target_date)
                ).rowcount
                
                session.commit()
                return deleted_count
            return 0
    
    def delete_job_by_id(self, job_id: int) -> bool:
        """Soft delete a specific job by ID."""
        with self.get_session() as session:
            job = session.get(Job, job_id)
            if not job:
                return False
            
            job.is_deleted = True
            job.deleted_at = datetime.utcnow()
            session.add(job)
            session.commit()
            return True

    def hard_delete_job_by_id(self, job_id: int) -> bool:
        """Hard delete a specific job by ID (for admin use)."""
        with self.get_session() as session:
            # First, delete associated job items
            session.exec(
                delete(JobItem).where(JobItem.job_id == job_id)
            )
            
            # Then delete the job
            result = session.exec(
                delete(Job).where(Job.id == job_id)
            )
            
            session.commit()
            return result.rowcount > 0

    def defer_job_to_next_day(self, job_id: int, new_priority: Optional[int] = None) -> bool:
        """Defer a job to the next day with optional priority update."""
        with self.get_session() as session:
            job = session.get(Job, job_id)
            if not job:
                return False
            
            # Convert date string to date object, add 1 day, convert back
            from datetime import datetime, timedelta
            current_date = datetime.strptime(job.date, '%Y-%m-%d').date()
            next_date = current_date + timedelta(days=1)
            job.date = next_date.strftime('%Y-%m-%d')
            
            if new_priority is not None:
                job.priority = new_priority
            
            session.add(job)
            session.commit()
            return True

    def bulk_defer_jobs(self, job_ids: List[int], priority_updates: Optional[Dict[int, int]] = None) -> int:
        """Defer multiple jobs to the next day with optional priority updates."""
        if not job_ids:
            return 0
            
        count = 0
        for job_id in job_ids:
            new_priority = priority_updates.get(job_id) if priority_updates else None
            if self.defer_job_to_next_day(job_id, new_priority):
                count += 1
        return count
    
    # Route assignment operations
    def get_route_assignments_by_date(self, target_date: str) -> List[RouteAssignment]:
        """Get all route assignments for a date with eager loading."""
        with self.get_session() as session:
            from sqlalchemy.orm import selectinload
            
            assignments = session.exec(
                select(RouteAssignment)
                .options(selectinload(RouteAssignment.truck))
                .where(RouteAssignment.date == target_date)
            ).all()
            
            # Force loading while session is active
            for assignment in assignments:
                _ = assignment.truck
            
            return assignments
    
    def create_route_assignment(
        self, 
        assignment_data: Dict[str, Any]
    ) -> RouteAssignment:
        """Create a route assignment."""
        with self.get_session() as session:
            assignment = RouteAssignment(**assignment_data)
            session.add(assignment)
            session.commit()
            session.refresh(assignment)
            return assignment
    
    def create_route_stop(self, stop_data: Dict[str, Any]) -> RouteStop:
        """Create a route stop."""
        with self.get_session() as session:
            stop = RouteStop(**stop_data)
            session.add(stop)
            session.commit()
            session.refresh(stop)
            return stop
    
    def delete_route_assignments_by_date(self, target_date: str) -> int:
        """Delete all route assignments for a date."""
        with self.get_session() as session:
            # Get assignment IDs
            assignment_ids = session.exec(
                select(RouteAssignment.id).where(
                    RouteAssignment.date == target_date
                )
            ).all()
            
            if assignment_ids:
                # Delete route stops first
                session.exec(
                    delete(RouteStop).where(
                        RouteStop.route_assignment_id.in_(assignment_ids)
                    )
                )
                
                # Delete assignments
                deleted_count = session.exec(
                    delete(RouteAssignment).where(
                        RouteAssignment.date == target_date
                    )
                ).rowcount
                
                session.commit()
                return deleted_count
            return 0
    
    def get_route_stops_by_assignment(self, assignment_id: int) -> List[RouteStop]:
        """Get all route stops for a route assignment with eager loading."""
        with self.get_session() as session:
            from sqlalchemy.orm import selectinload
            
            stops = session.exec(
                select(RouteStop)
                .options(
                    selectinload(RouteStop.job).selectinload(Job.location)
                )
                .where(RouteStop.route_assignment_id == assignment_id)
            ).all()
            
            # Force loading while session is active
            for stop in stops:
                _ = stop.job
                _ = stop.job.location
            
            return stops

    def delete_route_stops_from_order(self, assignment_id: int, start_order: int) -> int:
        """Delete route stops for a given assignment where stop_order >= start_order.

        Returns the number of deleted rows.
        """
        with self.get_session() as session:
            result = session.exec(
                delete(RouteStop).where(
                    (RouteStop.route_assignment_id == assignment_id) &
                    (RouteStop.stop_order >= start_order)
                )
            )
            session.commit()
            return result.rowcount or 0
    
    # Unassigned jobs operations
    def get_unassigned_jobs_by_date(self, target_date: str) -> List[UnassignedJob]:
        """Get unassigned jobs for a date."""
        with self.get_session() as session:
            return session.exec(
                select(UnassignedJob).where(UnassignedJob.date == target_date)
            ).all()
    
    def create_unassigned_job(self, unassigned_data: Dict[str, Any]) -> UnassignedJob:
        """Create an unassigned job record."""
        with self.get_session() as session:
            unassigned = UnassignedJob(**unassigned_data)
            session.add(unassigned)
            session.commit()
            session.refresh(unassigned)
            return unassigned
    
    def delete_unassigned_jobs_by_date(self, target_date: str) -> int:
        """Delete all unassigned jobs for a date."""
        with self.get_session() as session:
            deleted_count = session.exec(
                delete(UnassignedJob).where(UnassignedJob.date == target_date)
            ).rowcount
            session.commit()
            return deleted_count
    
    # Utility operations
    def health_check(self) -> bool:
        """Check if database is accessible."""
        try:
            with self.get_session() as session:
                session.exec(select(1))
                return True
        except Exception:
            return False

    # =========================
    # Drivers & Dispatch CRUD
    # =========================
    # Drivers
    def get_drivers(self) -> List[Driver]:
        with self.get_session() as session:
            return session.exec(select(Driver).where(Driver.active == True)).all()

    def upsert_driver(self, data: Dict[str, Any]) -> Driver:
        with self.get_session() as session:
            name = data.get("name")
            driver = session.exec(select(Driver).where(Driver.name == name)).first()
            if driver:
                for k, v in data.items():
                    setattr(driver, k, v)
            else:
                driver = Driver(**data)
                session.add(driver)
            session.commit()
            session.refresh(driver)
            return driver

    def get_driver_by_id(self, driver_id: int) -> Optional[Driver]:
        with self.get_session() as session:
            return session.get(Driver, driver_id)

    # Dispatch state
    def get_or_create_dispatch_state(self, driver_id: int, date: str) -> DriverDispatchState:
        with self.get_session() as session:
            state = session.exec(
                select(DriverDispatchState).where(
                    (DriverDispatchState.driver_id == driver_id) & (DriverDispatchState.date == date)
                )
            ).first()
            if state is None:
                state = DriverDispatchState(driver_id=driver_id, date=date, current_batch_index=0)
                session.add(state)
                session.commit()
                session.refresh(state)
            return state

    def set_dispatch_batch_index(self, driver_id: int, date: str, batch_index: int) -> DriverDispatchState:
        with self.get_session() as session:
            state = session.exec(
                select(DriverDispatchState).where(
                    (DriverDispatchState.driver_id == driver_id) & (DriverDispatchState.date == date)
                )
            ).first()
            if state is None:
                state = DriverDispatchState(driver_id=driver_id, date=date, current_batch_index=batch_index)
            else:
                state.current_batch_index = batch_index
            session.add(state)
            session.commit()
            session.refresh(state)
            return state

    def update_dispatch_state(self, driver_id: int, date: str, **fields: Any) -> DriverDispatchState:
        with self.get_session() as session:
            state = session.exec(
                select(DriverDispatchState).where(
                    (DriverDispatchState.driver_id == driver_id) & (DriverDispatchState.date == date)
                )
            ).first()
            if state is None:
                state = DriverDispatchState(driver_id=driver_id, date=date, current_batch_index=0)
            for k, v in fields.items():
                if hasattr(state, k):
                    setattr(state, k, v)
            session.add(state)
            session.commit()
            session.refresh(state)
            return state

    # Batch stops
    def set_dispatch_batch(self, driver_id: int, date: str, batch_index: int, stops: List[Dict[str, Any]]) -> List[DispatchBatchStop]:
        with self.get_session() as session:
            # Delete existing for this batch
            existing = session.exec(
                select(DispatchBatchStop).where(
                    (DispatchBatchStop.driver_id == driver_id) &
                    (DispatchBatchStop.date == date) &
                    (DispatchBatchStop.batch_index == batch_index)
                )
            ).all()
            for e in existing:
                session.delete(e)
            session.commit()

            created: List[DispatchBatchStop] = []
            for i, s in enumerate(stops):
                rec = DispatchBatchStop(
                    driver_id=driver_id,
                    date=date,
                    batch_index=batch_index,
                    seq_in_batch=i,
                    job_id=int(s["job_id"]),
                    expected_arrival=s.get("expected_arrival"),
                    expected_departure=s.get("expected_departure"),
                )
                session.add(rec)
                created.append(rec)
            session.commit()
            for c in created:
                session.refresh(c)
            return created

    def get_dispatch_batch(self, driver_id: int, date: str, batch_index: int) -> List[DispatchBatchStop]:
        with self.get_session() as session:
            return session.exec(
                select(DispatchBatchStop).where(
                    (DispatchBatchStop.driver_id == driver_id) &
                    (DispatchBatchStop.date == date) &
                    (DispatchBatchStop.batch_index == batch_index)
                ).order_by(DispatchBatchStop.seq_in_batch.asc())
            ).all()

    def mark_batch_stop_completed(self, batch_stop_id: int) -> Optional[DispatchBatchStop]:
        with self.get_session() as session:
            rec = session.get(DispatchBatchStop, batch_stop_id)
            if not rec:
                return None
            from datetime import datetime as _dt
            rec.completed_at = _dt.utcnow()
            session.add(rec)
            session.commit()
            session.refresh(rec)
            return rec

    def list_dispatch_batches(self, driver_id: int, date: str) -> Dict[int, List[DispatchBatchStop]]:
        """Return all batches for a driver/date keyed by batch_index."""
        with self.get_session() as session:
            rows = session.exec(
                select(DispatchBatchStop).where(
                    (DispatchBatchStop.driver_id == driver_id)
                    & (DispatchBatchStop.date == date)
                ).order_by(DispatchBatchStop.batch_index.asc(), DispatchBatchStop.seq_in_batch.asc())
            ).all()
            result: Dict[int, List[DispatchBatchStop]] = {}
            for r in rows:
                result.setdefault(r.batch_index, []).append(r)
            return result

    # Messaging logs
    def log_message(self, data: Dict[str, Any]) -> DispatchMessage:
        with self.get_session() as session:
            msg = DispatchMessage(**data)
            session.add(msg)
            session.commit()
            session.refresh(msg)
            return msg

    # Time tracking logs
    def create_time_log(self, data: Dict[str, Any]) -> DispatchTimeLog:
        """Create a new time log entry."""
        with self.get_session() as session:
            log_entry = DispatchTimeLog(**data)
            session.add(log_entry)
            session.commit()
            session.refresh(log_entry)
            return log_entry

    def update_time_log_actual(self, job_id: int, driver_id: int, date: str, 
                              actual_start: Optional[datetime] = None,
                              actual_end: Optional[datetime] = None) -> Optional[DispatchTimeLog]:
        """Update actual times for an existing time log entry."""
        with self.get_session() as session:
            log_entry = session.exec(
                select(DispatchTimeLog).where(
                    (DispatchTimeLog.job_id == job_id)
                    & (DispatchTimeLog.driver_id == driver_id)
                    & (DispatchTimeLog.date == date)
                )
            ).first()
            
            if log_entry:
                if actual_start:
                    log_entry.actual_start = actual_start
                    if log_entry.planned_start:
                        log_entry.delta_start_minutes = (actual_start - log_entry.planned_start).total_seconds() / 60
                
                if actual_end:
                    log_entry.actual_end = actual_end
                    if log_entry.planned_end:
                        log_entry.delta_end_minutes = (actual_end - log_entry.planned_end).total_seconds() / 60
                
                session.add(log_entry)
                session.commit()
                session.refresh(log_entry)
            
            return log_entry

    def list_time_logs(self, date: Optional[str] = None, driver_id: Optional[int] = None) -> List[DispatchTimeLog]:
        """List time logs with optional filtering."""
        with self.get_session() as session:
            query = select(DispatchTimeLog)
            
            if date:
                query = query.where(DispatchTimeLog.date == date)
            if driver_id:
                query = query.where(DispatchTimeLog.driver_id == driver_id)
            
            return session.exec(query.order_by(DispatchTimeLog.created_at.desc())).all()
