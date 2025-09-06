from __future__ import annotations

import importlib
from dataclasses import asdict, dataclass
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Any, Mapping, Optional, Union, cast

import yaml
from ml_collections import ConfigDict

from rem.constants import (
    CONFIG_FLAT_FILENAME,
    MANIFEST_FILENAME,
    REP_PREFIX,
    SUBCONFIG_FILENAME,
    SWEEP_PREFIX,
)
from rem.core.config import (
    flatten_config,
    load_config_from_yaml,
    override_config,
    save_config_to_yaml,
    to_dict,
)
from rem.core.config_validation import validate_config
from rem.core.experiment import ExperimentBase
from rem.core.manifest import (
    GroupManifest,
    RepManifest,
    SweepManifest,
    init_group_manifest,
    init_rep_manifest,
    init_sweep_manifest,
    summarize_group_status,
    summarize_sweep_status,
    update_group_manifest,
    update_rep_manifest,
    update_sweep_manifest,
)
from rem.core.registry import RegistryManager
from rem.core.stamp import create_group_stamp, format_rep_id, parse_group_id
from rem.core.status import VALID_STATUSES, is_terminal
from rem.core.sweeps import get_sweep_elements
from rem.utils.logger import get_logger
from rem.utils.paths import (
    get_default_events_path,
    get_group_dir,
    get_group_manifest_path,
    get_rep_dir,
    get_rep_manifest_path,
    get_sweep_dir,
    get_sweep_manifest_path,
)
from rem.utils.ulid import timestamp_from_ulid

logger = get_logger(__name__)


def _as_plain_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, ConfigDict):
        return to_dict(obj)
    if isinstance(obj, Mapping):
        return dict(obj)
    return {}


def _resolve_group_date_from_group_id(group_id: str) -> datetime:
    """
    Derive a timezone-aware UTC datetime from the group_id (which embeds ULID).
    The group directory helpers expect a date/datetime, while our stamp helper
    returns a display date string. We reconstruct the ULID from the group_id and
    then recover the original timestamp.
    """
    ulid_str = parse_group_id(group_id)  # 26-char
    return timestamp_from_ulid(ulid_str)


def _import_experiment_class(module_path: str, class_name: str) -> type[ExperimentBase]:
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    if not issubclass(cls, ExperimentBase):
        raise TypeError(f"{class_name} in {module_path} must subclass ExperimentBase")
    return cast(type[ExperimentBase], cls)


def prepare_config_for_run(
    cfg: ConfigDict, sweep_overrides: dict[str, Any]
) -> ConfigDict:
    """
    Apply per-element sweep overrides (and future test overrides) just-in-time.
    This doesn't mutate the incoming config.
    """
    if not sweep_overrides:
        return cfg
    # flat/dotted overrides are supported by override_config
    return override_config(cfg, sweep_overrides)


def stage_group(
    *,
    cfg: ConfigDict,
    group_id: str,
    test: bool = False,
    registry: Optional[RegistryManager] = None,
) -> Path:
    """
    Create the group directory and initial group manifest.
    Appends a CREATE_GROUP event to the registry.
    Returns the group directory path.
    """
    group_dt = _resolve_group_date_from_group_id(group_id)
    group_dir = get_group_dir(group_id, group_dt, test=test)
    group_dir.mkdir(parents=True, exist_ok=True)

    group_manifest_path = get_group_manifest_path(str(group_dir), group_dt, test=test)

    exp_path = cfg.get("experiment_path", "")
    exp_class = cfg.get("experiment_class", "Experiment")
    sweep_cfg = _as_plain_dict(cfg.get("sweep"))
    slurm_cfg = _as_plain_dict(cfg.get("slurm"))

    gm = GroupManifest(
        stamp=group_dt.isoformat(),
        group_id=group_id,
        experiment_class=str(exp_class),
        experiment_path=str(exp_path),
        sweep=sweep_cfg,
        slurm=slurm_cfg,
    )
    init_group_manifest(group_manifest_path, gm)

    if registry:
        registry.append_event(
            {
                "type": "CREATE_GROUP",
                "group_id": group_id,
                "timestamp": datetime.now().isoformat() + "Z",
            }
        )

    return group_dir


def stage_sweep(
    *,
    cfg: ConfigDict,
    group_id: str,
    sweep_id: str,
    parameter_overrides: dict[str, Any],
    num_reps: int,
    test: bool = False,
    registry: Optional[RegistryManager] = None,
) -> Path:
    group_dt = _resolve_group_date_from_group_id(group_id)
    sweep_dir = get_sweep_dir(group_id, group_dt, sweep_id, test=test)
    sweep_dir.mkdir(parents=True, exist_ok=True)

    reps_meta = [
        {"rep_id": format_rep_id(i + 1), "status": "PENDING", "version": 1}
        for i in range(max(num_reps, 1))
    ]

    sm = SweepManifest(
        sweep_id=sweep_id,
        parameter_combination=parameter_overrides,
        num_reps=len(reps_meta),
        reps=reps_meta,
        status="PENDING",
    )
    init_sweep_manifest(
        get_sweep_manifest_path(group_id, group_dt, sweep_id, test=test), sm
    )
    if registry:
        registry.append_event(
            {
                "type": "SUBMIT_SWEEP",
                "group_id": group_id,
                "sweep_id": sweep_id,
                "timestamp": datetime.now().isoformat() + "Z",
                "num_reps": len(reps_meta),
                "parameters": parameter_overrides,
            }
        )
    return sweep_dir


def stage_rep(
    *,
    base_cfg: ConfigDict,
    group_id: str,
    sweep_id: str,
    rep_id: str,
    sweep_overrides: dict[str, Any],
    test: bool = False,
) -> tuple[Path, Path]:
    """
    Create the rep directory, write subconfig and init rep manifest.
    Returns (rep_dir, subconfig_path).
    """
    group_dt = _resolve_group_date_from_group_id(group_id)
    rep_dir = get_rep_dir(group_id, group_dt, sweep_id, rep_id, test=test)
    rep_dir.mkdir(parents=True, exist_ok=True)

    resolved_cfg = prepare_config_for_run(base_cfg, sweep_overrides)
    subconfig_path = rep_dir.joinpath(SUBCONFIG_FILENAME)
    save_config_to_yaml(resolved_cfg, subconfig_path)

    # Optional: stash a flat dump for easy debugging/diffing
    flat_cfg_path = rep_dir.joinpath(CONFIG_FLAT_FILENAME)
    try:
        flat = flatten_config(resolved_cfg)
        with open(flat_cfg_path, "w") as f:
            yaml.safe_dump({k: v for k, v in flat.items()}, f)
    except Exception as e:
        logger.warning(
            f"Failed to write flat config for {rep_id} at {flat_cfg_path}: {e}"
        )

    rm = RepManifest(
        rep_id=rep_id,
        sweep_id=sweep_id,
        group_id=group_id,
        status="PENDING",
        parameters=sweep_overrides,
    )
    init_rep_manifest(
        get_rep_manifest_path(group_id, group_dt, sweep_id, rep_id, test=test),
        rm,
    )

    return rep_dir, subconfig_path


def run_single_rep(
    *,
    cfg: ConfigDict,
    group_id: str,
    sweep_id: str,
    rep_id: str,
    experiment_path: str,
    experiment_class: str,
    test: bool = False,
) -> None:
    group_dt = _resolve_group_date_from_group_id(group_id)
    rep_manifest_path = get_rep_manifest_path(
        group_id, group_dt, sweep_id, rep_id, test=test
    )

    # Mark as RUNNING
    update_rep_manifest(
        rep_manifest_path,
        {"status": "RUNNING", "timestamp_start": datetime.now().isoformat() + "Z"},
    )

    # Instantiate experiment
    try:
        expcls = _import_experiment_class(experiment_path, experiment_class)
        experiment = expcls(cfg)
        results = experiment.run()
        artifacts = results if isinstance(results, dict) else {"results": results}

        update_rep_manifest(
            rep_manifest_path,
            {
                "status": "COMPLETED",
                "timestamp_end": datetime.now().isoformat() + "Z",
                "artifacts": artifacts,
            },
        )
    except Exception as e:
        logger.exception(f"Experiment {rep_id} in sweep {sweep_id} crashed: {e}")
        update_rep_manifest(
            rep_manifest_path,
            {"status": "CRASHED", "timestamp_end": datetime.now().isoformat() + "Z"},
        )


def run_local(
    config: Union[ConfigDict, str, Path],
    *,
    overrides: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Run an experiment "locally" without staging any directories, manifests or registry updates.
    This is primarily for debugging and quick tests, not for production use.
    """
    # Load config if a path is given
    if isinstance(config, (str, Path)):
        cfg = load_config_from_yaml(config)
    else:
        cfg = config

    # Apply overrides if any
    if overrides:
        cfg = override_config(cfg, overrides)
        logger.info(f"Applied overrides: {overrides}")

    exp_path = str(cfg.get("experiment_path", "")).strip()
    exp_class = str(cfg.get("experiment_class", "Experiment")).strip()
    if not exp_path or not exp_class:
        raise ValueError(
            "Both experiment_path and experiment_class must be specified in the config."
        )

    expcls = _import_experiment_class(exp_path, exp_class)
    experiment = expcls(cfg)  # type: ignore[call-arg]
    results = experiment.run()
    return results if isinstance(results, dict) else {"results": results}


class MainRunner:
    """
    High-level orchestrator for staging and execution.

    This class is the main sauce of the entire project, performing full disk staging
    even for dry runs, so directories and manifests exist and can be reused for later
    invocations. It also handles loading the config, validating it, and applying overrides.
    """

    def __init__(
        self,
        *,
        test: bool = False,
        dryrun: bool = False,
        events_path: Path | None = None,
    ) -> None:
        self.test = test
        self.dryrun = dryrun
        events_path = events_path or get_default_events_path(test=self.test)
        self.registry = RegistryManager(events_path=events_path)

    def start(
        self,
        config_path: Path | str,
        *,
        reps_per_sweep: int = 1,
        group_id: Optional[str] = None,
    ) -> str:
        """
        Entry point. Returns the generated group_id.

        - Loads and validates config.
        - Creates a new group (ULID-based ID).
        - Stages sweeps and reps on disk.
        - If not dryrun, executes all reps sequentially. (Placeholder, later for SLURM.)
        """
        cfg = load_config_from_yaml(config_path)
        validate_config(cfg)

        #########################################################
        # LOGIC FOR RESUMING EXISTING GROUPS

        if group_id is not None:
            logger.info(f"Resuming existing group {group_id}")
            ulid_str = parse_group_id(group_id)
            group_dt = timestamp_from_ulid(ulid_str)
            group_dir = get_group_dir(group_id, group_dt, test=self.test)
            if not group_dir.exists():
                raise FileNotFoundError(
                    f"Group directory {group_dir} does not exist for group_id {group_id}"
                )

            posted_group_running = False

            for sweep_dir in sorted(
                p
                for p in group_dir.iterdir()
                if p.is_dir() and p.name.startswith(SWEEP_PREFIX)
            ):
                sweep_id = sweep_dir.name
                sm_path = get_sweep_manifest_path(
                    group_id, group_dt, sweep_id, test=self.test
                )
                if not sm_path.exists():
                    # if a sweep manifest is missing (shouldn't normally happen), skip it
                    logger.warning(
                        f"Sweep manifest {sm_path} missing, skipping sweep {sweep_id}"
                    )
                    continue
                sweep_manifest = SweepManifest.load(sm_path)
                element_overrides = sweep_manifest.parameter_combination or {}

                for rep_dir in sorted(
                    p
                    for p in sweep_dir.iterdir()
                    if p.is_dir() and p.name.startswith(REP_PREFIX)
                ):
                    rep_id = rep_dir.name
                    rm_path = get_rep_manifest_path(
                        group_id, group_dt, sweep_id, rep_id, test=self.test
                    )
                    if not rm_path.exists():
                        logger.warning(
                            f"Rep manifest {rm_path} missing, skipping rep {rep_id}"
                        )
                        continue
                    rep_manifest = RepManifest.load(rm_path)
                    if is_terminal(rep_manifest.status):
                        logger.info(
                            f"Rep {rep_id} in sweep {sweep_id} is already complete."
                        )
                        continue

                    # Load staged subconfig if present, else use base config + overrides
                    subconfig_path = rep_dir.joinpath(SUBCONFIG_FILENAME)
                    if subconfig_path.exists():
                        subcfg = load_config_from_yaml(subconfig_path)
                    else:
                        subcfg = prepare_config_for_run(cfg, element_overrides)

                    if not posted_group_running:
                        self.registry.append_event(
                            {
                                "type": "UPDATE_STATUS",
                                "group_id": group_id,
                                "timestamp": datetime.now().isoformat() + "Z",
                                "status": "RUNNING",
                            }
                        )
                        posted_group_running = True

                    # Run the rep
                    exp_path = str(cfg.get("experiment_path", ""))
                    exp_class = str(cfg.get("experiment_class", "Experiment"))
                    run_single_rep(
                        cfg=subcfg,
                        group_id=group_id,
                        sweep_id=sweep_id,
                        rep_id=rep_id,
                        experiment_path=exp_path,
                        experiment_class=exp_class,
                        test=self.test,
                    )

                # After attempting reps, summarize sweep status and log one element-level UPDATE_STATUS event
                # (Reload each rep manifest to get final statuses)
                rep_entries = []
                for rep_dir in sorted(
                    p
                    for p in sweep_dir.iterdir()
                    if p.is_dir() and p.name.startswith(REP_PREFIX)
                ):
                    rep_dirname = rep_dir.name
                    rm_path = get_rep_manifest_path(
                        group_id, group_dt, sweep_id, rep_dirname, test=self.test
                    )
                    if rm_path.exists():
                        rep_manifest = RepManifest.load(rm_path)
                        rep_entries.append(
                            {
                                "rep_id": rep_manifest.rep_id,
                                "status": rep_manifest.status,
                            }
                        )
                sweep_status = summarize_sweep_status(rep_entries)
                update_sweep_manifest(sm_path, {"status": sweep_status})
                self.registry.append_event(
                    {
                        "type": "UPDATE_STATUS",
                        "group_id": group_id,
                        "sweep_id": sweep_id,
                        "timestamp": datetime.now().isoformat() + "Z",
                        "status": sweep_status,
                    }
                )

            # Summarize final group status
            sweep_manifest_paths = sorted(
                glob(
                    str(
                        get_group_dir(group_id, group_dt, test=self.test).joinpath(
                            "S_*", MANIFEST_FILENAME
                        )
                    )
                )
            )
            sweeps = [SweepManifest.load(Path(path)) for path in sweep_manifest_paths]
            group_status = summarize_group_status(sweeps)
            update_group_manifest(
                get_group_manifest_path(group_id, group_dt, test=self.test),
                {"status": group_status},
            )
            self.registry.append_event(
                {
                    "type": "UPDATE_STATUS",
                    "group_id": group_id,
                    "timestamp": datetime.now().isoformat() + "Z",
                    "status": group_status,
                }
            )

            return group_id

        #########################################################
        # LOGIC FOR NEW GROUPS

        group_id, _display_date = create_group_stamp()
        group_dt = _resolve_group_date_from_group_id(group_id)

        logger.info(f"Created group {group_id} @ {group_dt.date().isoformat()}")

        # Stage group and write manifest + registry event
        stage_group(
            cfg=cfg,
            group_id=group_id,
            test=self.test,
            registry=self.registry,
        )

        # Expand sweep elements
        sweep_elements = get_sweep_elements(to_dict(cfg))
        if not sweep_elements:
            sweep_elements = [("S_0001", {})]  # single sweep, no params

        # Stage sweeps and reps
        for sweep_id, overrides in sweep_elements:
            stage_sweep(
                cfg=cfg,
                group_id=group_id,
                sweep_id=sweep_id,
                parameter_overrides=overrides,
                num_reps=reps_per_sweep,
                test=self.test,
                registry=self.registry,
            )
            for r in range(reps_per_sweep):
                rep_id = format_rep_id(r + 1)
                stage_rep(
                    base_cfg=cfg,
                    group_id=group_id,
                    sweep_id=sweep_id,
                    rep_id=rep_id,
                    sweep_overrides=overrides,
                    test=self.test,
                )

        # If dryrun, stop here
        if self.dryrun:
            logger.info(
                "Dryrun complete. Staged group, sweeps, and reps on disk without execution."
            )
            return group_id

        exp_path = str(cfg.get("experiment_path", ""))
        exp_class = str(cfg.get("experiment_class", "Experiment"))

        posted_group_running = False
        for sweep_id, overrides in sweep_elements:
            # mark sweep start
            group_dt = _resolve_group_date_from_group_id(group_id)
            sm_path = get_sweep_manifest_path(
                group_id, group_dt, sweep_id, test=self.test
            )
            update_sweep_manifest(sm_path, {"status": "RUNNING"})
            for r in range(reps_per_sweep):
                rep_id = format_rep_id(r + 1)
                # subconfig is the prepared config for this specific (sweep, rep) element
                subconfig = prepare_config_for_run(cfg, overrides)

                # Post a single group-level RUNNING event when the first rep starts
                if not posted_group_running:
                    self.registry.append_event(
                        {
                            "type": "UPDATE_STATUS",
                            "group_id": group_id,
                            "timestamp": datetime.now().isoformat() + "Z",
                            "status": "RUNNING",
                        }
                    )
                    posted_group_running = True

                run_single_rep(
                    cfg=subconfig,
                    group_id=group_id,
                    sweep_id=sweep_id,
                    rep_id=rep_id,
                    experiment_path=exp_path,
                    experiment_class=exp_class,
                    test=self.test,
                )

            sweep_manifest = SweepManifest.load(sm_path)
            # Reload each rep manifest to summarize final sweep status
            rep_entries = []
            for r in range(reps_per_sweep):
                rep_id = format_rep_id(r + 1)
                rm_path = get_rep_manifest_path(
                    group_id, group_dt, sweep_id, rep_id, test=self.test
                )
                rep_manifest = RepManifest.load(rm_path)
                rep_entries.append(
                    {
                        "rep_id": rep_manifest.rep_id,
                        "status": rep_manifest.status,
                    }
                )
            sweep_status = summarize_sweep_status(rep_entries)
            update_sweep_manifest(sm_path, {"status": sweep_status})

            # Append a sweep-level status update to the registry
            self.registry.append_event(
                {
                    "type": "UPDATE_STATUS",
                    "group_id": group_id,
                    "sweep_id": sweep_id,
                    "timestamp": datetime.now().isoformat() + "Z",
                    "status": sweep_status,
                }
            )

        # Summarize final group status
        # Load all sweep manifests in this group
        group_dt = _resolve_group_date_from_group_id(group_id)
        sweep_manifest_paths = sorted(
            glob(
                str(
                    get_group_dir(group_id, group_dt, test=self.test).joinpath(
                        "S_*", MANIFEST_FILENAME
                    )
                )
            )
        )
        sweeps = [SweepManifest.load(Path(path)) for path in sweep_manifest_paths]
        group_status = summarize_group_status(sweeps)
        update_group_manifest(
            get_group_manifest_path(group_id, group_dt, test=self.test),
            {"status": group_status},
        )
        self.registry.append_event(
            {
                "type": "UPDATE_STATUS",
                "group_id": group_id,
                "timestamp": datetime.now().isoformat() + "Z",
                "status": group_status,
            }
        )

        return group_id
