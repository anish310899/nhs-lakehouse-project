"""Names used across the notebooks, kept in one place."""

from __future__ import annotations

from dataclasses import dataclass

ENTITIES = ["clinicians", "patients", "referrals", "contacts"]


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "workspace"
    schema: str = "nhs_lakehouse"
    volume: str = "raw"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def landing_path(self) -> str:
        return f"{self.volume_path}/landing"

    def table(self, name: str) -> str:
        """Fully qualified Unity Catalog table name, e.g. workspace.nhs_lakehouse.silver_patients."""
        return f"{self.catalog}.{self.schema}.{name}"
