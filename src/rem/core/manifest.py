import json
import tempfile
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Type, TypeVar, Union, cast

from rem.core.status import TERMINAL_STATUSES, VALID_STATUSES
from rem.utils.lock import FileLock
from rem.utils.logger import get_logger

logger = get_logger(__name__)

ManifestType = TypeVar("ManifestType", bound="BaseManifest")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BaseManifest:
    def save(self, path: Path) -> None:
        if not is_dataclass(self):
            raise TypeError(f"{self.__class__.__name__} must be a dataclass instance")
        if hasattr(self, "timestamp_updated"):
            setattr(self, "timestamp_updated", utc_now_iso())
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(path):
            with tempfile.NamedTemporaryFile(
                "w", delete=False, dir=path.parent, suffix=".tmp"
            ) as tmp:
                json.dump(asdict(cast(Any, self)), tmp, indent=2)
                tmp_path = Path(tmp.name)
            tmp_path.replace(path)
        logger.info(f"Saved manifest to {path}")

    @classmethod
    def load(cls: Type[ManifestType], path: Path) -> ManifestType:
        data = json.loads(path.read_text())
        logger.info(f"Loaded manifest from {path}")
        return cls(**data)  # type: ignore

    def to_dict(self) -> dict[str, Any]:
        if not is_dataclass(self):
            raise TypeError(f"{self.__class__.__name__} must be a dataclass instance")
        return asdict(cast(Any, self))


@dataclass
class RepManifest(BaseManifest):
    rep_id: str
    sweep_id: str
    group_id: str
    version: int = 1
    patch_id: Optional[str] = None
    replaces: Optional[str] = None
    timestamp_created: str = field(default_factory=utc_now_iso)
    timestamp_updated: Optional[str] = None
    timestamp_start: Optional[str] = None
    timestamp_end: Optional[str] = None
    status: str = "PENDING"
    artifacts: dict[str, str] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    system_info: dict[str, str] = field(default_factory=dict)


@dataclass
class SweepManifest(BaseManifest):
    sweep_id: str
    parameter_combination: dict[str, Any]
    num_reps: int
    reps: list[dict[str, Any]]
    status: str = "PENDING"
    timestamp_created: str = field(default_factory=utc_now_iso)
    timestamp_updated: Optional[str] = None


@dataclass
class GroupManifest(BaseManifest):
    stamp: str
    group_id: str
    experiment_class: str
    experiment_path: Union[str, Path]
    sweep: dict[str, Any]
    slurm: dict[str, Any]
    timestamp_created: str = field(default_factory=utc_now_iso)
    timestamp_updated: Optional[str] = None
    patches: list[dict[str, Any]] = field(default_factory=list)
    status: str = "PENDING"


def init_rep_manifest(path: Path, manifest: RepManifest) -> None:
    manifest.save(path)
    logger.info(f"Initialized rep manifest at {path}")


def init_sweep_manifest(path: Path, manifest: SweepManifest) -> None:
    manifest.save(path)
    logger.info(f"Initialized sweep manifest at {path}")


def init_group_manifest(path: Path, manifest: GroupManifest) -> None:
    manifest.save(path)
    logger.info(f"Initialized group manifest at {path}")


def update_rep_manifest(path: Path, updates: dict[str, Any]) -> None:
    rep = RepManifest.load(path)
    for k, v in updates.items():
        if k == "status" and v not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{v}' for rep manifest.")
        setattr(rep, k, v)
    rep.save(path)
    logger.info(f"Updated rep manifest at {path} with {updates}")


def update_sweep_manifest(path: Path, updates: dict[str, Any]) -> None:
    sweep = SweepManifest.load(path)
    for k, v in updates.items():
        if k == "status" and v not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{v}' for sweep manifest.")
        setattr(sweep, k, v)
    sweep.save(path)
    logger.info(f"Updated sweep manifest at {path} with {updates}")


def update_group_manifest(path: Path, updates: dict[str, Any]) -> None:
    group = GroupManifest.load(path)
    for k, v in updates.items():
        if k == "status" and v not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{v}' for group manifest.")
        setattr(group, k, v)
    group.save(path)
    logger.info(f"Updated group manifest at {path} with {updates}")


def save_manifest(path: Path, manifest: BaseManifest) -> None:
    manifest.save(path)
    logger.info(f"Manifest saved to {path}")


def load_manifest(path: Path, manifest_type: Type[ManifestType]) -> ManifestType:
    manifest = manifest_type.load(path)
    logger.info(f"Manifest loaded from {path}")
    return manifest


def summarize_sweep_status(rep_entries: list[dict[str, Any]]) -> str:
    statuses = [r["status"] for r in rep_entries]
    if all(s == "COMPLETED" for s in statuses):
        return "COMPLETED"
    elif all(s == "PENDING" for s in statuses):
        return "PENDING"
    elif any(s not in TERMINAL_STATUSES for s in statuses):
        return "IN_PROGRESS"
    else:
        return "PARTIAL_COMPLETION"


def summarize_group_status(sweep_manifests: list[SweepManifest]) -> str:
    statuses = [sweep.status for sweep in sweep_manifests]
    if all(s == "COMPLETED" for s in statuses):
        return "COMPLETED"
    elif all(s == "PENDING" for s in statuses):
        return "PENDING"
    elif any(s not in TERMINAL_STATUSES for s in statuses):
        return "IN_PROGRESS"
    else:
        return "PARTIAL_COMPLETION"


def summarize_group_patches(rep_manifests: list[RepManifest]) -> list[dict[str, Any]]:
    patches: dict[str, dict[str, Any]] = {}
    for rep in rep_manifests:
        if rep.patch_id:
            patches.setdefault(
                rep.patch_id,
                {
                    "patch_id": rep.patch_id,
                    "replaces": [],
                    "timestamp": rep.timestamp_end,
                },
            )
            if rep.replaces:
                patches[rep.patch_id]["replaces"].append(rep.replaces)
    return list(patches.values())
