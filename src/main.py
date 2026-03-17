#!/usr/bin/env python3
"""
OMNI-MASTER v6.0 - Linux Package Intelligence Console
Integrated script: Parts 1 + 2 + 3 merged into a single deployable file.
Tested for Kali-Termux environments (ASCII-safe, UTF-8 clean).
"""

# ==============================================================================
# SECTION 1 - IMPORTS (all consolidated, zero external dependencies)
# ==============================================================================

import os
import re
import sys
import json
import time
import math
import shutil
import tempfile
import threading
import subprocess
import importlib.metadata
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

try:
    from packaging.version import Version, InvalidVersion
    HAS_PACKAGING = True
except ImportError:
    HAS_PACKAGING = False


# ==============================================================================
# SECTION 2 - ANSI TERMINAL PALETTE (ASCII-safe escape codes only)
# ==============================================================================

R  = "\033[31m"   # Red
G  = "\033[32m"   # Green
Y  = "\033[33m"   # Yellow
B  = "\033[34m"   # Blue
C  = "\033[36m"   # Cyan
W  = "\033[37m"   # White
M  = "\033[35m"   # Magenta
N  = "\033[0m"    # Reset
DIM  = "\033[2m"
BOLD = "\033[1m"


# ==============================================================================
# SECTION 3 - SYSTEM CONSTANTS
# ==============================================================================

GITHUB_MAP = {
    "sherlock": "sherlock-project/sherlock",
    "sqlmap":   "sqlmapproject/sqlmap",
}

DPKG_LOCK_PATH       = "/var/lib/dpkg/lock-frontend"
CACHE_TTL_SECONDS    = 30
MIN_DISK_BYTES       = 1 * 1024 * 1024 * 1024
MIN_RAM_PERCENT      = 5.0
LEVENSHTEIN_THRESHOLD = 2
CHECKPOINT_PATH      = os.path.expanduser("~/.omni_checkpoint")
BACKOFF_BASE_SECONDS = 2
BACKOFF_MULTIPLIER   = 2
BACKOFF_MAX_RETRIES  = 5

# Session-wide selection queue and lazy domain cache
_selected_assets: set = set()
_domain_cache:    dict = {}


# ==============================================================================
# SECTION 4 - SMART CACHE (TTL-aware, thread-safe)
# ==============================================================================

@dataclass
class CacheEntry:
    status:    int
    timestamp: datetime = field(default_factory=datetime.now)


class SmartCache:
    """
    Thread-safe TTL cache for package status results.
    Entries expire after CACHE_TTL_SECONDS so changes from another
    terminal surface automatically without a manual refresh.
    """

    def __init__(self, ttl: int = CACHE_TTL_SECONDS):
        self._store: dict = {}
        self._ttl   = timedelta(seconds=ttl)
        self._lock  = threading.Lock()

    def get(self, key: str) -> Optional[int]:
        with self._lock:
            entry = self._store.get(key)
            if entry and (datetime.now() - entry.timestamp) < self._ttl:
                return entry.status
        return None

    def set(self, key: str, status: int) -> None:
        with self._lock:
            self._store[key] = CacheEntry(status=status)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def invalidate_many(self, keys: list) -> None:
        with self._lock:
            for k in keys:
                self._store.pop(k, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_cache = SmartCache()


# ==============================================================================
# SECTION 5 - CORE UI UTILITIES
# ==============================================================================

def clear_screen() -> None:
    """Wipe the terminal using ANSI escape sequences (no subprocess)."""
    sys.stdout.write("\033[H\033[2J\033[3J")
    sys.stdout.flush()


def banner(sys_total=None, title: str = "MASTER-CONSOLE") -> None:
    """
    High-visibility ASCII header for professional deployment.
    Zero Arabic characters for maximum compatibility.
    """
    t = str(sys_total) if sys_total is not None else "?"
    print(f"""{R}
    ██╗  ██╗ █████╗ ██╗     ██╗     ██████╗ ███████╗
    ██║ ██╔╝██╔══██╗██║     ██║     ██╔══██╗██╔════╝
    █████╔╝ ███████║██║     ██║     ██║  ██║█████╗  
    ██╔═██╗ ██╔══██║██║     ██║     ██║  ██║██╔══╝  
    ██║  ██╗██║  ██║███████╗███████╗██████╔╝███████╗
    ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═════╝ ╚══════╝
    {C}      [ IDENTITY: {W}OMNI-MASTER-V6.0 {C}| STATUS: {G}PROFESSIONAL {C}]
    {G}      [ REGISTRY: {W}{t:>7} PACKAGES {G}| SCOPE: {Y}{title} {G}]{N}""")



def status_badge(code: int) -> str:
    """Return a coloured status marker: green=current, yellow=outdated, dim=absent."""
    if code == 1:
        return f"{G}[+]{N}"
    if code == 2:
        return f"{Y}[^]{N}"
    return f"{DIM}[ ]{N}"


def selected_tag(pkg: str) -> str:
    """Return a [SELECTED] marker if the package is in the active queue."""
    return f" {M}[SELECTED]{N}" if pkg in _selected_assets else ""


# ==============================================================================
# SECTION 6 - INPUT SANITISATION
# ==============================================================================

def sanitize_package_name(raw: str) -> Optional[str]:
    """
    Validate a string against the Debian package-name specification.
    Returns the lowercased name or None on invalid input.
    Callers MUST test the return value for None before proceeding.
    """
    if not raw or not isinstance(raw, str):
        return None
    name = raw.strip().lower()
    if re.fullmatch(r'[a-z0-9][a-z0-9.+\-]*', name):
        return name
    return None


# ==============================================================================
# SECTION 7 - ROBUST VERSION COMPARISON
# ==============================================================================

def _version_tuple(v: str) -> tuple:
    """
    Heuristic version string to integer tuple.
    Handles epochs (1:2.3), distro tags (2.0-ubuntu1), pre-release (1.2b-3).
    """
    v = re.sub(r'\d+:', '', v)
    v = re.sub(r'[-~][a-zA-Z].*$', '', v)
    return tuple(int(p) for p in re.split(r'[^0-9]+', v) if p.isdigit())


def safe_version_gte(v1: str, v2: str) -> bool:
    """
    Return True if v1 >= v2. Never raises.
    Tries packaging.version first, falls back to tuple heuristic.
    """
    if HAS_PACKAGING:
        try:
            return Version(v1) >= Version(v2)
        except Exception:
            pass
    try:
        return _version_tuple(v1) >= _version_tuple(v2)
    except Exception:
        return False


# ==============================================================================
# SECTION 8 - HARDENED SUBPROCESS WRAPPER
# ==============================================================================

def run_cmd(
    args:    list,
    timeout: int  = 10,
    env:     Optional[dict] = None,
    capture: bool = True,
    check:   bool = False,
) -> tuple:
    """
    Universal subprocess wrapper. No shell=True anywhere.
    Returns (stdout, stderr, returncode). Never raises.
    Exit codes: 124=timeout, 126=permission denied, 127=not found.
    """
    try:
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            text=True,
            timeout=timeout,
            env=env,
            check=check,
        )
        return (result.stdout or ""), (result.stderr or ""), result.returncode
    except subprocess.TimeoutExpired:
        return "", "TimeoutExpired", 124
    except PermissionError:
        return "", "PermissionError", 126
    except FileNotFoundError as exc:
        return "", str(exc), 127
    except subprocess.CalledProcessError as exc:
        return (exc.stdout or ""), (exc.stderr or ""), exc.returncode
    except Exception as exc:
        return "", str(exc), 1


# ==============================================================================
# SECTION 9 - SYSTEM GUARDIAN (pre-flight resource monitor)
# ==============================================================================

@dataclass
class GuardianReport:
    """Structured result returned by SystemGuardian.preflight()."""
    disk_ok:      bool
    ram_ok:       bool
    disk_free_gb: float
    ram_free_pct: float
    warnings:     list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.disk_ok and self.ram_ok


class SystemGuardian:
    """
    Silent background resource monitor.
    Uses shutil.disk_usage() for disk and /proc/meminfo for RAM.
    No root privileges required.
    """

    def __init__(
        self,
        min_disk_bytes:  int   = MIN_DISK_BYTES,
        min_ram_percent: float = MIN_RAM_PERCENT,
    ):
        self._min_disk = min_disk_bytes
        self._min_ram  = min_ram_percent

    def _check_disk(self) -> tuple:
        try:
            usage   = shutil.disk_usage("/")
            free_gb = usage.free / (1024 ** 3)
            return (usage.free >= self._min_disk, free_gb)
        except OSError:
            return (False, 0.0)

    def _check_ram(self) -> tuple:
        """
        Parses /proc/meminfo directly.
        Uses MemAvailable (accounts for reclaimable cache) over MemFree.
        """
        try:
            meminfo: dict = {}
            with open("/proc/meminfo") as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) >= 2:
                        meminfo[parts[0].rstrip(":")] = int(parts[1])
            total     = meminfo.get("MemTotal", 0)
            available = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
            if total == 0:
                return (False, 0.0)
            pct = (available / total) * 100.0
            return (pct >= self._min_ram, round(pct, 1))
        except (OSError, ValueError):
            return (False, 0.0)

    def preflight(self) -> GuardianReport:
        """Run all checks and return a GuardianReport."""
        disk_ok, disk_gb = self._check_disk()
        ram_ok,  ram_pct = self._check_ram()
        warnings = []
        if not disk_ok:
            warnings.append(
                f"LOW DISK: {disk_gb:.2f} GB free "
                f"(min {self._min_disk / (1024**3):.0f} GB required)"
            )
        if not ram_ok:
            warnings.append(
                f"LOW RAM: {ram_pct:.1f}% available "
                f"(min {self._min_ram:.0f}% required)"
            )
        return GuardianReport(
            disk_ok=disk_ok, ram_ok=ram_ok,
            disk_free_gb=disk_gb, ram_free_pct=ram_pct,
            warnings=warnings,
        )

    def status_line(self) -> str:
        """
        Returns a compact status string for UI headers.
        Format:  [ DISK:48.3GB  RAM:62.4%  SEL:3 ]
        """
        _, disk_gb = self._check_disk()
        _, ram_pct = self._check_ram()
        sel        = len(_selected_assets)
        dc = G if disk_gb >= (self._min_disk / (1024 ** 3)) else R
        rc = G if ram_pct >= self._min_ram else R
        sc = M if sel > 0 else DIM
        return (
            f"{DIM}[ "
            f"{dc}DISK:{disk_gb:.1f}GB{N}{DIM}  "
            f"{rc}RAM:{ram_pct:.1f}%{N}{DIM}  "
            f"{sc}SEL:{sel}{N}{DIM} ]{N}"
        )


# ==============================================================================
# SECTION 10 - ERROR REGISTRY (structured failure log)
# ==============================================================================

@dataclass
class ErrorRecord:
    """One structured failure event stored by ErrorRegistry."""
    timestamp:  str
    operation:  str
    package:    str
    stderr:     str
    suggested:  str
    returncode: int


class ErrorRegistry:
    """
    Collects structured error records from installation operations.
    Parses apt/pip stderr to identify the culprit package and suggest a fix.
    The UI only surfaces this registry when it is non-empty.
    """

    _PATTERNS = [
        (re.compile(r"Unable to fetch some archives"),
         "Run: apt-get update  -- mirrors may be stale."),
        (re.compile(r"dpkg.*dependency problems"),
         "Run: apt-get install -f  -- broken dependency chain."),
        (re.compile(r"Package '(.+?)' has no installation candidate"),
         "Package absent from sources. Try: apt-cache search <term>."),
        (re.compile(r"E: Unable to locate package (.+)"),
         "Package not found. Verify name or add the correct repo/PPA."),
        (re.compile(r"Sub-process.*returned an error code"),
         "Run: dpkg --configure -a  -- interrupted previous install."),
        (re.compile(r"Permission denied"),
         "Root privileges required. Re-run as root."),
        (re.compile(r"No space left on device"),
         "Free disk: rm -rf /var/cache/apt/archives/*."),
        (re.compile(r"pip.*externally-managed"),
         "Use pipx or a venv -- system Python is PEP-668 protected."),
    ]

    def __init__(self):
        self._records: list = []
        self._lock = threading.Lock()

    def _parse_stderr(self, stderr: str, package: str) -> tuple:
        for pattern, hint in self._PATTERNS:
            match = pattern.search(stderr)
            if match:
                culprit = match.group(1).strip() if match.lastindex else package
                return culprit, hint
        return package, "Check: journalctl -xe  or  apt-get install -f"

    def log(self, operation: str, package: str, stderr: str, rc: int) -> None:
        culprit, suggestion = self._parse_stderr(stderr, package)
        record = ErrorRecord(
            timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            operation  = operation.upper(),
            package    = culprit,
            stderr     = stderr[:400],
            suggested  = suggestion,
            returncode = rc,
        )
        with self._lock:
            self._records.append(record)

    @property
    def is_empty(self) -> bool:
        return len(self._records) == 0

    @property
    def count(self) -> int:
        return len(self._records)

    def all_records(self) -> list:
        with self._lock:
            return list(self._records)

    def clear(self) -> None:
        with self._lock:
            self._records.clear()


# ==============================================================================
# SECTION 11 - DPKG LOCK GUARD
# ==============================================================================

def _inode(path: str) -> Optional[int]:
    try:
        return os.stat(path).st_ino
    except OSError:
        return None


def dpkg_lock_held() -> Optional[str]:
    """
    Reads /proc/locks to find which process holds the DPKG frontend lock.
    Returns "process (PID N)" or None if the lock is free.
    """
    target = _inode(DPKG_LOCK_PATH)
    if target is None:
        return None
    try:
        with open("/proc/locks") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) < 8:
                    continue
                try:
                    if int(parts[7].split(":")[-1]) == target:
                        pid  = parts[4]
                        comm = f"/proc/{pid}/comm"
                        name = (
                            open(comm).read().strip()
                            if os.path.exists(comm) else "unknown"
                        )
                        return f"{name} (PID {pid})"
                except (ValueError, IndexError):
                    continue
    except OSError:
        pass
    return None


def assert_dpkg_free() -> bool:
    """Returns True when DPKG lock is free; prints error and returns False otherwise."""
    holder = dpkg_lock_held()
    if holder:
        print(f"\n{R}[!] DPKG LOCK HELD BY: {Y}{holder}{N}")
        print(f"{R}    Wait for the other package manager to finish.{N}\n")
        return False
    return True


# ==============================================================================
# SECTION 12 - ROOT GUARD (operation-scoped)
# ==============================================================================

def require_root(operation: str = "this operation") -> None:
    """
    Exit with an error if the process is not running as root.
    Called only by write operations (install, upgrade). Never at startup.
    """
    if os.getuid() != 0:
        print(f"{R}[!] ROOT REQUIRED FOR: {operation.upper()}{N}")
        sys.exit(1)


# ==============================================================================
# SECTION 13 - EXPONENTIAL BACKOFF (network resilience)
# ==============================================================================

def exponential_backoff_retry(
    func,
    *args,
    max_retries:  int   = BACKOFF_MAX_RETRIES,
    base_seconds: float = BACKOFF_BASE_SECONDS,
    multiplier:   float = BACKOFF_MULTIPLIER,
    exceptions:   tuple = (OSError, urllib.request.URLError),
    label:        str   = "network operation",
    **kwargs,
):
    """
    Execute func(*args, **kwargs) with exponential backoff on network errors.

    Wait formula:  delay = base * multiplier^(attempt-1)  (capped at 60s)
    Default delays with base=2, mult=2: 2s, 4s, 8s, 16s, 32s

    Raises the last caught exception when all retries are exhausted.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except exceptions as exc:
            last_exc    = exc
            wait        = min(base_seconds * (multiplier ** (attempt - 1)), 60.0)
            remaining   = max_retries - attempt
            print(
                f"{Y}[!] {label} failed (attempt {attempt}/{max_retries}). "
                f"Retrying in {wait:.0f}s "
                f"({remaining} attempt{'s' if remaining != 1 else ''} left)...{N}"
            )
            time.sleep(wait)

    print(f"{R}[-] {label} permanently failed after {max_retries} attempts.{N}")
    raise last_exc


# ==============================================================================
# SECTION 14 - NETWORK UTILITIES
# ==============================================================================

def get_remote_version(repo: str) -> str:
    """Fetch the latest release tag from GitHub API."""
    try:
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            return data.get("tag_name", "").lstrip("v")
    except Exception:
        return ""


# ==============================================================================
# SECTION 15 - PACKAGE STATUS CHECK
# ==============================================================================

def get_pkg_status(package: str) -> int:
    """
    Returns: 0=not installed  1=up-to-date  2=outdated

    Uses dpkg-query -W for real-time local version detection (avoids
    stale apt-cache data). Result is TTL-cached to prevent redundant calls.
    Cache is explicitly invalidated after any install or upgrade.
    """
    safe = sanitize_package_name(package)
    if safe is None:
        return 0

    cached = _cache.get(safe)
    if cached is not None:
        return cached

    def _store(v: int) -> int:
        _cache.set(safe, v)
        return v

    target_path = f"/root/{safe}"

    local_v, _, _ = run_cmd(["dpkg-query", "-W", "-f=${Version}", safe], timeout=5)
    local_v = local_v.strip()

    pip_v = ""
    try:
        pip_pkg = "sherlock-project" if safe == "sherlock" else safe
        pip_v   = importlib.metadata.version(pip_pkg)
    except importlib.metadata.PackageNotFoundError:
        pass

    active_local_v = (
        pip_v if pip_v and (not local_v or safe_version_gte(pip_v, local_v))
        else local_v
    )

    if safe in GITHUB_MAP:
        remote_v = get_remote_version(GITHUB_MAP[safe])
        if remote_v and active_local_v:
            return _store(1 if safe_version_gte(active_local_v, remote_v) else 2)
        if not active_local_v:
            return _store(0)

    if os.path.isdir(os.path.join(target_path, ".git")):
        original = os.getcwd()
        os.chdir(target_path)
        try:
            run_cmd(["git", "fetch"], timeout=6)
            lh, _, _ = run_cmd(["git", "rev-parse", "@"])
            rh, _, _ = run_cmd(["git", "rev-parse", "@{u}"])
            if lh.strip() and rh.strip() and lh.strip() != rh.strip():
                os.chdir(original)
                return _store(2)
        except Exception:
            pass
        os.chdir(original)
        return _store(1)

    if not active_local_v:
        return _store(0)

    policy, _, _ = run_cmd(["apt-cache", "policy", safe], timeout=8)
    inst = re.search(r"Installed:\s+(\S+)", policy)
    cand = re.search(r"Candidate:\s+(\S+)", policy)
    if (inst and cand
            and inst.group(1) not in ("(none)", "")
            and inst.group(1) != cand.group(1)):
        return _store(2)

    return _store(1)


# ==============================================================================
# SECTION 16 - PEP-668 SAFE PYTHON TOOL UPGRADE
# ==============================================================================

def upgrade_python_tool_safe(tool: str, github_url: str) -> tuple:
    """
    Upgrade a Python CLI tool without --break-system-packages.
    Strategy A: pipx (preferred isolated install).
    Strategy B: temporary venv with binary copy to /usr/local/bin.
    Returns (success: bool, stderr: str).
    """
    if shutil.which("pipx"):
        print(f"{C}[*] STRATEGY: pipx isolated environment{N}")
        _, err, rc = run_cmd(
            ["pipx", "install", "--force", f"git+{github_url}"],
            timeout=120, capture=False,
        )
        if rc == 0:
            return True, ""
        _, err2, rc2 = run_cmd(["pipx", "upgrade", tool], timeout=120, capture=False)
        return (rc2 == 0), (err2 if rc2 != 0 else "")

    print(f"{Y}[*] pipx unavailable. STRATEGY: temporary venv{N}")
    tmp_dir = tempfile.mkdtemp(prefix=f"omni_{tool}_")
    try:
        _, err, rc = run_cmd([sys.executable, "-m", "venv", tmp_dir], timeout=30)
        if rc != 0:
            return False, err

        venv_pip = os.path.join(tmp_dir, "bin", "pip")
        _, err, rc = run_cmd(
            [venv_pip, "install", "--upgrade", f"git+{github_url}"],
            timeout=120, capture=False,
        )
        if rc != 0:
            return False, err

        venv_bin = os.path.join(tmp_dir, "bin", tool)
        if os.path.isfile(venv_bin):
            dest = f"/usr/local/bin/{tool}"
            shutil.copy2(venv_bin, dest)
            os.chmod(dest, 0o755)
            return True, ""

        return False, f"Binary not found at {venv_bin} after install"

    except Exception as exc:
        return False, str(exc)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ==============================================================================
# SECTION 17 - CHECKPOINT MANAGER (atomic state / disaster recovery)
# ==============================================================================

class CheckpointManager:
    """
    Atomic JSON-based transaction log for batch package installations.

    File format (.omni_checkpoint):
    {
      "session_id":  "2025-01-15T14:32:00",
      "created_at":  "2025-01-15T14:32:00",
      "updated_at":  "2025-01-15T14:35:12",
      "queue": [
        {"package": "nmap",   "status": "done",    "attempted_at": "..."},
        {"package": "sqlmap", "status": "pending", "attempted_at": null}
      ]
    }

    Writes are atomic: temp file + os.replace() prevents corruption on
    power loss mid-write (POSIX guarantee on same filesystem).
    """

    STATUS_PENDING     = "pending"
    STATUS_IN_PROGRESS = "in_progress"
    STATUS_DONE        = "done"
    STATUS_FAILED      = "failed"

    def __init__(self, path: str = CHECKPOINT_PATH):
        self._path = path
        self._lock = threading.RLock()

    def _load(self) -> Optional[dict]:
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return None

    def _save(self, data: dict) -> None:
        dir_name = os.path.dirname(self._path) or "."
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".omni_tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            os.replace(tmp_path, self._path)
        except OSError:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _now(self) -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def begin_session(self, packages: list) -> None:
        """Create a new checkpoint with all packages in pending state."""
        with self._lock:
            now  = self._now()
            data = {
                "session_id": now,
                "created_at": now,
                "updated_at": now,
                "queue": [
                    {"package": pkg, "status": self.STATUS_PENDING, "attempted_at": None}
                    for pkg in packages
                ],
            }
            self._save(data)

    def mark_started(self, package: str) -> None:
        with self._lock:
            data = self._load()
            if not data:
                return
            for entry in data["queue"]:
                if entry["package"] == package:
                    entry["status"]       = self.STATUS_IN_PROGRESS
                    entry["attempted_at"] = self._now()
                    break
            data["updated_at"] = self._now()
            self._save(data)

    def mark_done(self, package: str) -> None:
        with self._lock:
            data = self._load()
            if not data:
                return
            for entry in data["queue"]:
                if entry["package"] == package:
                    entry["status"] = self.STATUS_DONE
                    break
            data["updated_at"] = self._now()
            self._save(data)
            incomplete = [
                e for e in data["queue"]
                if e["status"] in (self.STATUS_PENDING, self.STATUS_IN_PROGRESS)
            ]
            if not incomplete:
                self.close_session()

    def mark_failed(self, package: str) -> None:
        with self._lock:
            data = self._load()
            if not data:
                return
            for entry in data["queue"]:
                if entry["package"] == package:
                    entry["status"] = self.STATUS_FAILED
                    break
            data["updated_at"] = self._now()
            self._save(data)

    def close_session(self) -> None:
        with self._lock:
            try:
                os.unlink(self._path)
            except FileNotFoundError:
                pass

    def get_pending(self) -> list:
        with self._lock:
            data = self._load()
            if not data:
                return []
            return [
                e["package"] for e in data["queue"]
                if e["status"] in (self.STATUS_PENDING, self.STATUS_IN_PROGRESS)
            ]

    def has_incomplete_session(self) -> bool:
        return len(self.get_pending()) > 0

    def get_session_summary(self) -> Optional[dict]:
        with self._lock:
            data = self._load()
            if not data:
                return None
            queue = data.get("queue", [])
            return {
                "created_at": data.get("created_at", "unknown"),
                "total":   len(queue),
                "done":    sum(1 for e in queue if e["status"] == self.STATUS_DONE),
                "pending": sum(1 for e in queue
                               if e["status"] in (self.STATUS_PENDING,
                                                  self.STATUS_IN_PROGRESS)),
                "failed":  sum(1 for e in queue if e["status"] == self.STATUS_FAILED),
            }

    def check_resume(self) -> Optional[list]:
        """
        Boot-time entry point. Detects an incomplete session and asks the user
        whether to resume. Called from main() before the menu loop.
        Returns a list of pending package names or None.
        """
        if not self.has_incomplete_session():
            return None
        summary = self.get_session_summary()
        if not summary:
            return None

        print(f"\n{Y}+--------------------------------------------------+{N}")
        print(f"{Y}|  [!] INCOMPLETE INSTALLATION DETECTED            |{N}")
        print(f"{Y}+--------------------------------------------------+{N}")
        print(f"  Session started : {summary['created_at']}")
        print(f"  Total packages  : {summary['total']}")
        print(f"  Completed       : {G}{summary['done']}{N}")
        print(f"  Remaining       : {R}{summary['pending']}{N}")
        print(f"  Failed          : {R}{summary['failed']}{N}")
        print(f"{Y}+--------------------------------------------------+{N}")

        try:
            answer = input(f"\n{C}>> Resume interrupted installation? [Y/n]: {N}").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return None

        if answer in ("", "y", "yes"):
            pending = self.get_pending()
            print(f"{G}[+] Resuming {len(pending)} package(s)...{N}\n")
            return pending
        else:
            print(f"{DIM}[*] Checkpoint discarded.{N}")
            self.close_session()
            return None


# Module-level singleton used by all deployment functions
checkpoint = CheckpointManager()


# ==============================================================================
# SECTION 18 - LEVENSHTEIN FUZZY SEARCH ENGINE
# ==============================================================================

def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Wagner-Fischer dynamic programming edit distance.
    Space-optimised to O(min(m,n)) using two rolling rows.
    """
    if len(s1) > len(s2):
        s1, s2 = s2, s1
    m, n   = len(s1), len(s2)
    prev   = list(range(n + 1))
    curr   = [0] * (n + 1)

    for i in range(1, m + 1):
        curr[0] = i
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                curr[j] = prev[j - 1]
            else:
                curr[j] = 1 + min(prev[j], curr[j - 1], prev[j - 1])
        prev, curr = curr, [0] * (n + 1)

    return prev[n]


def fuzzy_match(query: str, candidate: str,
                threshold: int = LEVENSHTEIN_THRESHOLD) -> Optional[int]:
    """
    Test whether candidate is a fuzzy match for query.
    Uses three fast-path checks before falling back to full DP.
    Returns the integer distance if within threshold, else None.
    """
    q, c = query.lower().strip(), candidate.lower().strip()
    if q == c:
        return 0
    if q in c or c in q:
        return 1
    if len(q) >= 3 and c.startswith(q[:3]):
        dist = levenshtein_distance(q, c)
        return dist if dist <= threshold else None
    if abs(len(q) - len(c)) > threshold:
        return None
    dist = levenshtein_distance(q, c)
    return dist if dist <= threshold else None


def fuzzy_search(query: str, candidates: list,
                 threshold: int = LEVENSHTEIN_THRESHOLD) -> list:
    """
    Return (package_name, edit_distance) tuples ranked by distance ASC.
    Only includes candidates within threshold edits of query.
    """
    hits: list = []
    q_lower = query.lower().strip()
    for candidate in candidates:
        dist = fuzzy_match(q_lower, candidate, threshold)
        if dist is not None:
            hits.append((candidate, dist))
    hits.sort(key=lambda x: (x[1], x[0]))
    return hits


# ==============================================================================
# SECTION 19 - SMART CONTEXTUAL RECOMMENDER
# ==============================================================================

def _detect_query_domain(query: str) -> Optional[int]:
    """
    Map a query to the most relevant DOMAIN_DEFS entry by scoring
    how many specialty tags appear as sub-tokens in the query.
    Returns domain_id (1-20) or None.
    """
    q_lower = query.lower()
    best_id, best_score = None, 0
    for domain_id, meta in DOMAIN_DEFS.items():
        score = sum(
            1 for tag in meta.get("specialties", [])
            if tag.replace("-", " ") in q_lower
            or tag.replace("-", "") in q_lower
            or any(w in q_lower for w in tag.split("-"))
        )
        if score > best_score:
            best_score, best_id = score, domain_id
    return best_id if best_score > 0 else None


def _scrape_apt_metadata(package_name: str) -> dict:
    """
    Pull Description, Section, Suggests, Recommends from apt-cache show.
    Returns a dict with those four keys (empty strings on failure).
    """
    meta = {"description": "", "section": "", "suggests": "", "recommends": ""}
    try:
        result = subprocess.run(
            ["apt-cache", "show", package_name],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=6,
        )
        output = result.stdout
        desc = re.search(r'^Description(?:-[a-z]+)?:\s*(.+)$', output, re.MULTILINE)
        if desc:
            meta["description"] = desc.group(1).strip()
        for field_name in ("Section", "Suggests", "Recommends"):
            match = re.search(rf'^{field_name}:\s*(.+)$', output, re.MULTILINE)
            if match:
                meta[field_name.lower()] = match.group(1).strip()
    except (subprocess.TimeoutExpired, OSError):
        pass
    return meta


def _score_candidate(query: str, package_name: str,
                     meta: dict, specialty_tags: list) -> int:
    """
    Score a candidate package against the original query.
    Rubric (additive): name match +4, description +3, section tag +2,
    description tag +2, in suggests/recommends +1.
    """
    score   = 0
    q_lower = query.lower()
    desc    = meta.get("description", "").lower()
    section = meta.get("section",     "").lower()
    if q_lower in package_name.lower():
        score += 4
    if q_lower in desc:
        score += 3
    for tag in specialty_tags:
        tag_words = tag.replace("-", " ")
        if tag_words in section or tag.replace("-", "") in section:
            score += 2
        if tag_words in desc:
            score += 2
    combined = (meta.get("suggests", "") + " " + meta.get("recommends", "")).lower()
    if q_lower in combined:
        score += 1
    return score


def get_recommendations(query: str, domain_id: Optional[int] = None,
                        max_results: int = 8) -> list:
    """
    Generate contextually relevant package recommendations for a failed query.

    Pipeline:
      1. Detect target domain from query (auto if not supplied).
      2. Run apt-cache search to get a broad candidate pool.
      3. Scrape apt metadata for each candidate.
      4. Score candidates using domain specialty tags.
      5. Return top max_results sorted by score DESC.
    """
    detected_id  = domain_id or _detect_query_domain(query)
    spec_tags: list = []
    if detected_id and detected_id in DOMAIN_DEFS:
        spec_tags = DOMAIN_DEFS[detected_id].get("specialties", [])

    try:
        result = subprocess.run(
            ["apt-cache", "search", query],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=15,
        )
        raw_lines = result.stdout.splitlines()
    except (subprocess.TimeoutExpired, OSError):
        return []

    candidates: list = []
    for line in raw_lines[:60]:
        parts = line.split(" - ", 1)
        if len(parts) == 2:
            candidates.append({"name": parts[0].strip(), "description": parts[1].strip()})

    if not candidates:
        return []

    scored: list = []
    for cand in candidates:
        meta  = _scrape_apt_metadata(cand["name"])
        if not meta["description"]:
            meta["description"] = cand["description"]
        score = _score_candidate(query, cand["name"], meta, spec_tags)
        if score > 0:
            scored.append({
                "name":        cand["name"],
                "description": meta["description"] or cand["description"],
                "score":       score,
            })

    scored.sort(key=lambda x: (-x["score"], x["name"]))
    return scored[:max_results]


# ==============================================================================
# SECTION 20 - DOMAIN DEFINITIONS (taxonomy with hidden specialties)
# ==============================================================================

DOMAIN_DEFS: dict = {
    1: {
        "name":        "System Core & Admin",
        "pattern":     r"^(systemd|coreutils|apt|bash|util-linux|cron)",
        "specialties": ["init-system", "process-management", "package-management",
                        "scheduled-tasks", "shell", "core-utilities"],
    },
    2: {
        "name":        "Software Libraries",
        "pattern":     r"^lib",
        "specialties": ["shared-library", "dynamic-linking", "runtime-dependency",
                        "development-library", "abi-compatibility"],
    },
    3: {
        "name":        "Dev & Compilers",
        "pattern":     r"(gcc|g\+\+|make|cmake|^clang)",
        "specialties": ["c-compiler", "cpp-compiler", "build-system",
                        "cross-compilation", "code-generation", "static-analysis"],
    },
    4: {
        "name":        "Python Ecosystem",
        "pattern":     r"^python",
        "specialties": ["python3", "pip", "virtualenv", "data-science",
                        "web-framework", "scripting", "automation"],
    },
    5: {
        "name":        "Perl & Ruby Ecosystem",
        "pattern":     r"^(perl|ruby)",
        "specialties": ["perl-module", "ruby-gem", "scripting",
                        "text-processing", "web-framework"],
    },
    6: {
        "name":        "Network & Protocols",
        "pattern":     r"(^net-tools|^iproute|ssh|curl|wget|tcpdump)",
        "specialties": ["network-configuration", "packet-capture", "ssh-client",
                        "http-client", "dns-tools", "firewall", "vpn",
                        "bandwidth-monitoring", "protocol-analysis"],
    },
    7: {
        "name":        "Web Infrastructure",
        "pattern":     r"(^apache2|^nginx|^php)",
        "specialties": ["web-server", "reverse-proxy", "load-balancer",
                        "php-runtime", "ssl-tls", "cgi", "wsgi"],
    },
    8: {
        "name":        "Database Engines",
        "pattern":     r"(sql|mongo|redis|postgre)",
        "specialties": ["relational-database", "nosql", "in-memory-cache",
                        "document-store", "key-value-store", "sql-client",
                        "database-migration", "orm"],
    },
    9: {
        "name":        "Cyber Security Tools",
        "pattern":     r"(kali-tools|^exploit|^nmap|metasploit|sherlock|sqlmap)",
        "specialties": [
            "web-audit", "sql-injection", "xss-detection",
            "web-fuzzing", "directory-brute-force", "ssl-audit",
            "network-recon", "port-scanning", "service-fingerprint",
            "os-detection", "network-mapping", "packet-injection",
            "osint", "username-recon", "email-harvesting",
            "domain-recon", "social-media-recon", "google-dorking",
            "exploit-framework", "post-exploitation", "payload-generation",
            "privilege-escalation", "lateral-movement",
            "password-cracking", "hash-cracking", "brute-force",
            "wordlist", "credential-stuffing",
            "wifi-audit", "wpa-cracking", "bluetooth-recon",
            "forensics", "binary-analysis", "malware-analysis",
            "reverse-engineering", "memory-forensics",
        ],
    },
    10: {
        "name":        "Desktop Environments",
        "pattern":     r"(^x11|^gnome|^kde|^xfce)",
        "specialties": ["display-server", "window-manager", "desktop-environment",
                        "compositor", "display-manager", "gtk", "qt"],
    },
    11: {
        "name":        "Science & Engineering",
        "pattern":     r"(science|math|physics)",
        "specialties": ["numerical-computing", "symbolic-math", "statistics",
                        "data-visualization", "simulation", "cad", "gis"],
    },
    12: {
        "name":        "Graphics Processing",
        "pattern":     r"(^gimp|image|graphics)",
        "specialties": ["image-editing", "vector-graphics", "3d-rendering",
                        "image-conversion", "screenshot", "screen-recording",
                        "color-management"],
    },
    13: {
        "name":        "Audio & Video",
        "pattern":     r"(audio|sound|video|ffmpeg)",
        "specialties": ["audio-player", "video-player", "media-converter",
                        "audio-editor", "video-editor", "streaming",
                        "codec", "screen-capture", "podcast"],
    },
    14: {
        "name":        "Hardware & Drivers",
        "pattern":     r"(firmware|^xserver-xorg-video)",
        "specialties": ["gpu-driver", "firmware-update", "hardware-monitor",
                        "usb-tools", "pci-tools", "bios-tools",
                        "power-management", "sensor-monitoring"],
    },
    15: {
        "name":        "Documentation",
        "pattern":     r"(^manpages|^doc)",
        "specialties": ["man-pages", "info-pages", "offline-docs",
                        "api-reference", "tutorial", "cheatsheet"],
    },
    16: {
        "name":        "Games",
        "pattern":     r"(^games)",
        "specialties": ["terminal-game", "puzzle", "strategy",
                        "arcade", "emulator", "game-engine"],
    },
    17: {
        "name":        "Text & Fonts",
        "pattern":     r"(^fonts|text|awk|sed)",
        "specialties": ["font-family", "text-processing", "stream-editor",
                        "grep", "awk", "markdown", "latex"],
    },
    18: {
        "name":        "Virtualization",
        "pattern":     r"(docker|virtualbox|qemu)",
        "specialties": ["container", "virtual-machine", "hypervisor",
                        "container-orchestration", "image-management",
                        "network-namespace", "sandbox"],
    },
    19: {
        "name":        "Terminal Utilities",
        "pattern":     r"(^tmux|^screen|htop|neofetch)",
        "specialties": ["terminal-multiplexer", "process-monitor", "system-info",
                        "file-manager", "disk-usage", "log-viewer",
                        "shell-enhancement", "productivity"],
    },
    20: {
        "name":        "Miscellaneous",
        "pattern":     r"(^misc|^dummy)",
        "specialties": ["uncategorized", "meta-package", "transitional"],
    },
}


# ==============================================================================
# SECTION 21 - LAZY DOMAIN LOADER
# ==============================================================================

def load_domain(domain_id: int) -> list:
    """
    Lazy-load a domain's package list on first access.
    Startup cost is zero: no domain is queried until the user selects it.
    Subsequent calls return the in-memory list instantly.
    """
    if domain_id in _domain_cache:
        return _domain_cache[domain_id]
    pattern = DOMAIN_DEFS[domain_id]["pattern"]
    print(f"{Y}[*] LOADING DOMAIN...{N}", end="\r")
    stdout, _, _ = run_cmd(["apt-cache", "search", pattern], timeout=20)
    pkgs = sorted({
        line.split()[0]
        for line in stdout.splitlines()
        if line.strip()
    })[:300]
    _domain_cache[domain_id] = pkgs
    return pkgs


def get_domain_count(domain_id: int) -> str:
    """Return formatted count if domain is loaded, else '???' to signal lazy state."""
    if domain_id in _domain_cache:
        return f"{len(_domain_cache[domain_id]):05d}"
    return " ??? "


# ==============================================================================
# SECTION 22 - SEARCH ENGINE (three-tier: exact -> fuzzy -> recommender)
# ==============================================================================

def _build_apt_candidate_pool(query: str) -> list:
    """Build a broad candidate list from apt-cache search for fuzzy fallback."""
    stdout, _, _ = run_cmd(["apt-cache", "search", query], timeout=15)
    return [
        line.split()[0]
        for line in stdout.splitlines()[:200]
        if line.strip()
    ]


def _detect_scope_domain(scope_list: list) -> int:
    """
    Identify which DOMAIN_DEFS entry a scope_list belongs to.
    Scores each domain by counting regex matches against the list.
    Returns domain_id or 9 (Security) as the safe default.
    """
    if not scope_list:
        return 9
    best_id, best_score = 9, 0
    for domain_id, meta in DOMAIN_DEFS.items():
        try:
            pattern = re.compile(meta["pattern"], re.IGNORECASE)
        except re.error:
            continue
        score = sum(1 for pkg in scope_list[:50] if pattern.search(pkg))
        if score > best_score:
            best_score, best_id = score, domain_id
    return best_id


def _render_results_page(results, query, title, sys_total,
                         page_offset, page_size=20):
    clear_screen()
    banner(sys_total, title)
    print(f"  {guardian.status_line()}\n")
    view = results[page_offset: page_offset + page_size]
    for i, pkg in enumerate(view):
        idx   = page_offset + i + 1
        code  = get_pkg_status(pkg)
        badge = status_badge(code)
        stag  = selected_tag(pkg)
        col   = C if query == pkg.lower() else W
        print(f"  {G}[{idx:02d}]{N} {col}{pkg.ljust(35)}{N} {badge}{stag}")
    shown = min(page_offset + page_size, len(results))
    nav   = ""
    if shown < len(results):
        nav += "  [N]ext"
    if page_offset > 0:
        nav += "  [B]ack"
    print(f"\n{DIM}  Showing {page_offset+1}-{shown} of {len(results)}{nav}{N}")


def _render_fuzzy_page(hits, query, sys_total, page_offset, page_size=20):
    clear_screen()
    banner(sys_total, "FUZZY-RESULTS")
    print(f"  {guardian.status_line()}")
    print(f"\n  {Y}No exact match for {W}\"{query}\"{Y}. "
          f"Closest fuzzy matches (delta<={LEVENSHTEIN_THRESHOLD}):{N}\n")
    view = hits[page_offset: page_offset + page_size]
    for i, (pkg, dist) in enumerate(view):
        idx   = page_offset + i + 1
        badge = status_badge(get_pkg_status(pkg))
        stag  = selected_tag(pkg)
        dc    = G if dist <= 1 else Y
        print(f"  {G}[{idx:02d}]{N} {W}{pkg.ljust(35)}{N} {badge} {dc}d{dist}{N}{stag}")
    shown = min(page_offset + page_size, len(hits))
    nav = ""
    if shown < len(hits):
        nav += "  [N]ext"
    if page_offset > 0:
        nav += "  [B]ack"
    print(f"\n{DIM}  Showing {page_offset+1}-{shown} of {len(hits)}{nav}{N}")


def _render_recommendations(recs, query, sys_total):
    clear_screen()
    banner(sys_total, "SMART-RECOMMENDATIONS")
    print(f"  {guardian.status_line()}")
    print(f"\n  {R}No results for {W}\"{query}\"{R}. "
          f"Contextually similar tools:{N}\n")
    if not recs:
        print(f"  {DIM}No recommendations available.{N}")
        return
    max_score = max(r["score"] for r in recs) or 1
    for i, rec in enumerate(recs, 1):
        filled = round((rec["score"] / max_score) * 10)
        bar    = f"{G}{'#' * filled}{DIM}{'.' * (10 - filled)}{N}"
        badge  = status_badge(get_pkg_status(rec["name"]))
        stag   = selected_tag(rec["name"])
        desc   = rec["description"][:55]
        print(f"  {C}[{i:02d}]{N} {W}{rec['name'].ljust(28)}{N} {bar} {badge}{stag}")
        print(f"       {DIM}{desc}{N}")


def search_engine(scope_list: list = None, sys_total=None) -> None:
    """
    Interactive three-tier package search.

    Tier 1 - EXACT:     substring match in scope_list or apt-cache.
    Tier 2 - FUZZY:     Levenshtein distance <= LEVENSHTEIN_THRESHOLD.
    Tier 3 - RECOMMEND: apt metadata scoring via get_recommendations().

    Multi-select: entering an ID toggles the package in _selected_assets.
    [D] triggers batch_deploy() on the full _selected_assets set.
    [N]/[B] paginate results.  [S] starts a new search.  [M] returns.
    """
    if sys_total is None:
        stdout, _, _ = run_cmd(["apt-cache", "stats"], timeout=10)
        m = re.search(r"Total package names:\s+(\d+)", stdout)
        sys_total = int(m.group(1)) if m else "?"

    scope_domain_id = _detect_scope_domain(scope_list) if scope_list else None

    while True:
        clear_screen()
        banner(sys_total, "BLEEDING-EDGE-SEARCH")
        print(f"  {guardian.status_line()}\n")
        print(f"  {DIM}Fuzzy matching active (delta<={LEVENSHTEIN_THRESHOLD}). "
              f"[M] to return.{N}\n")

        raw_input = input(f"  {Y}>> SEARCH: {N}").strip()
        if raw_input.lower() == "m":
            return

        safe_q = sanitize_package_name(raw_input)
        if safe_q is None:
            print(f"\n  {R}[!] Invalid input -- use standard package name characters.{N}")
            time.sleep(1.5)
            continue

        # --- Tier 1: Exact ---
        exact_results: list = []
        if scope_list is not None:
            exact_results = [p for p in scope_list if safe_q in p.lower()]
        else:
            stdout, _, _ = run_cmd(["apt-cache", "search", safe_q], timeout=15)
            exact_results = [
                line.split()[0]
                for line in stdout.splitlines()[:200]
                if line.strip()
            ]

        # --- Tier 2: Fuzzy ---
        fuzzy_hits: list = []
        if not exact_results:
            print(f"\n  {Y}[~] No exact match. Running fuzzy search...{N}",
                  end="", flush=True)
            fuzzy_pool = list(scope_list) if scope_list else _build_apt_candidate_pool(safe_q)
            for domain_pkgs in _domain_cache.values():
                for pkg in domain_pkgs:
                    if pkg not in fuzzy_pool:
                        fuzzy_pool.append(pkg)
            fuzzy_hits = fuzzy_search(safe_q, fuzzy_pool, LEVENSHTEIN_THRESHOLD)
            print(f" {G}{len(fuzzy_hits)} match(es).{N}" if fuzzy_hits else f" {R}none.{N}")

        # --- Tier 3: Recommend ---
        recommendations: list = []
        if not exact_results and not fuzzy_hits:
            print(f"\n  {C}[*] Consulting Smart Recommender...{N}", end="", flush=True)
            recommendations = get_recommendations(safe_q, domain_id=scope_domain_id)
            print(f" {G}{len(recommendations)} suggestion(s).{N}"
                  if recommendations else f" {R}none.{N}")

        if not exact_results and not fuzzy_hits and not recommendations:
            print(f"\n  {R}[!] No results or recommendations found for \"{safe_q}\".{N}")
            time.sleep(2.0)
            continue

        if exact_results:
            active_mode, active_results = "exact", exact_results
        elif fuzzy_hits:
            active_mode, active_results = "fuzzy", fuzzy_hits
        else:
            active_mode, active_results = "recommend", recommendations

        page_offset, page_size = 0, 20

        while True:
            if active_mode == "exact":
                _render_results_page(active_results, safe_q, "EXACT-RESULTS",
                                     sys_total, page_offset, page_size)
            elif active_mode == "fuzzy":
                _render_fuzzy_page(active_results, safe_q, sys_total,
                                   page_offset, page_size)
            else:
                _render_recommendations(active_results, safe_q, sys_total)

            total_items = len(active_results)
            can_next = (page_offset + page_size) < total_items
            can_back = page_offset > 0

            hints = [f"{DIM}ID=toggle"]
            if can_next: hints.append("[N]ext")
            if can_back: hints.append("[B]ack")
            if _selected_assets:
                hints.append(f"{M}[D]eploy({len(_selected_assets)}){DIM}")
            hints += ["[S]earch again", "[M]enu"]
            print(f"\n  {'  '.join(hints)}{N}")

            ans = input(f"  {C}>> {N}").strip().lower()

            if ans == "m":
                return
            if ans == "s":
                break
            if ans == "n" and can_next:
                page_offset += page_size
                continue
            if ans == "b" and can_back:
                page_offset -= page_size
                continue
            if ans == "d":
                if _selected_assets:
                    batch_deploy(_selected_assets.copy())
                else:
                    print(f"  {Y}[!] Nothing selected.{N}")
                    time.sleep(1.2)
                continue
            if ans.isdigit():
                idx = int(ans) - 1
                pkg_name = ""
                if active_mode == "exact" and 0 <= idx < len(active_results):
                    pkg_name = active_results[idx]
                elif active_mode == "fuzzy" and 0 <= idx < len(active_results):
                    pkg_name = active_results[idx][0]
                elif active_mode == "recommend" and 0 <= idx < len(active_results):
                    pkg_name = active_results[idx]["name"]
                if pkg_name:
                    v = sanitize_package_name(pkg_name)
                    if v:
                        if v in _selected_assets:
                            _selected_assets.discard(v)
                            print(f"  {DIM}[-] Deselected: {v}{N}")
                        else:
                            _selected_assets.add(v)
                            print(f"  {M}[+] Selected  : {v}{N}")
                        time.sleep(0.4)


# ==============================================================================
# SECTION 23 - DEPLOYMENT ENGINE (hybrid batch with checkpoint integration)
# ==============================================================================

def _run_preflight() -> bool:
    """Run SystemGuardian pre-flight. Returns True when safe to proceed."""
    report = guardian.preflight()
    if not report.passed:
        print(f"\n{R}+-- PRE-FLIGHT CHECK FAILED ----------------------------+{N}")
        for w in report.warnings:
            print(f"{R}|  [!] {w:<50} |{N}")
        print(f"{R}+------------------------------------------------------+{N}")
    return report.passed


def _partition_queue(packages: set) -> tuple:
    """
    Split packages into (apt_queue, github_queue).
    Invalid names are skipped with a warning.
    """
    apt_queue:    list = []
    github_queue: list = []
    for pkg in sorted(packages):
        safe = sanitize_package_name(pkg)
        if safe is None:
            print(f"  {Y}[!] Skipping invalid name: '{pkg}'{N}")
            continue
        if safe in GITHUB_MAP:
            github_queue.append(safe)
        else:
            apt_queue.append(safe)
    return apt_queue, github_queue


def _print_deploy_header(apt_queue: list, github_queue: list) -> None:
    clear_screen()
    banner(None, "HYBRID DEPLOY ENGINE")
    print(f"\n{B}--------------------------------------------------------------{N}")
    print(f"  {G}APT    queue : {W}{len(apt_queue):>3} package(s){N}")
    print(f"  {C}GITHUB queue : {W}{len(github_queue):>3} tool(s){N}")
    print(f"  {DIM}Checkpoint   : {CHECKPOINT_PATH}{N}")
    print(f"{B}--------------------------------------------------------------{N}\n")


def _print_deploy_summary(succeeded: list, failed: list) -> None:
    print(f"\n{B}--------------------------------------------------------------{N}")
    print(f"  {G}SUCCEEDED : {len(succeeded)}{N}")
    if failed:
        print(f"  {R}FAILED    : {len(failed)}  (see [E] error log){N}")
        for pkg in failed:
            print(f"     {DIM}-> {pkg}{N}")
    print(f"{B}--------------------------------------------------------------{N}")


def _install_apt_package(pkg: str, env: dict, index: int, total: int) -> tuple:
    """
    Install a single APT package with full checkpoint lifecycle tracking.
    Returns (success: bool, stderr: str).
    """
    print(f"  {C}[{index:>3}/{total}]{N} {W}{pkg}{N} ... ", end="", flush=True)
    checkpoint.mark_started(pkg)
    _, stderr, rc = run_cmd(
        ["apt-get", "install", "-y", pkg],
        timeout=180, capture=True, env=env,
    )
    if rc == 0:
        print(f"{G}OK{N}")
        checkpoint.mark_done(pkg)
        return True, ""
    else:
        print(f"{R}FAILED (rc={rc}){N}")
        checkpoint.mark_failed(pkg)
        error_registry.log("DEPLOY", pkg, stderr, rc)
        return False, stderr


def _install_github_tool(tool: str, index: int, total: int) -> tuple:
    """
    Install a GitHub-hosted tool with checkpoint + exponential backoff.
    Returns (success: bool, stderr: str).
    """
    github_url = f"https://github.com/{GITHUB_MAP[tool]}.git"
    print(f"  {C}[{index:>3}/{total}]{N} {W}{tool}{N} (GitHub) ... ", end="", flush=True)
    checkpoint.mark_started(tool)

    def _attempt() -> tuple:
        ok, stderr = upgrade_python_tool_safe(tool, github_url)
        if not ok and any(
            kw in stderr.lower()
            for kw in ("timeout", "connection", "network", "ssl", "urlopen")
        ):
            raise OSError(f"Network failure: {stderr}")
        return ok, stderr

    try:
        ok, stderr = exponential_backoff_retry(
            _attempt,
            label        = f"GitHub install: {tool}",
            max_retries  = BACKOFF_MAX_RETRIES,
            base_seconds = BACKOFF_BASE_SECONDS,
            multiplier   = BACKOFF_MULTIPLIER,
            exceptions   = (OSError,),
        )
    except OSError as exc:
        ok, stderr = False, str(exc)

    if ok:
        print(f"{G}OK{N}")
        checkpoint.mark_done(tool)
        return True, ""
    else:
        print(f"{R}FAILED{N}")
        checkpoint.mark_failed(tool)
        error_registry.log("DEPLOY", tool, stderr, 1)
        return False, stderr


def batch_deploy(packages: set) -> None:
    """
    Hybrid batch deployment with atomic state tracking.

    Phase 1: All APT packages as a single transaction.
             Falls back to per-package isolation on bulk failure.
    Phase 2: GitHub tools with per-tool PEP-668-safe install + backoff.

    Checkpoint tracks every package transition:
        pending -> in_progress -> done | failed
    """
    require_root("batch deploy")
    if not assert_dpkg_free():
        input(f"{C}Press ENTER...{N}")
        return
    if not _run_preflight():
        input(f"{C}Press ENTER to abort...{N}")
        return

    apt_queue, github_queue = _partition_queue(packages)
    all_packages = apt_queue + github_queue

    if not all_packages:
        print(f"\n{Y}[!] No valid packages to deploy.{N}")
        time.sleep(1.5)
        return

    checkpoint.begin_session(all_packages)
    _print_deploy_header(apt_queue, github_queue)

    succeeded: list = []
    failed:    list = []
    env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}

    # Phase 1: APT atomic transaction
    if apt_queue:
        print(f"{Y}[*] PHASE 1 -- APT ATOMIC INSTALL ({len(apt_queue)} packages){N}\n")
        apt_total = len(apt_queue)
        _, bulk_stderr, bulk_rc = run_cmd(
            ["apt-get", "install", "-y"] + apt_queue,
            timeout=600, capture=True, env=env,
        )
        if bulk_rc == 0:
            print(f"  {G}[OK] Bulk transaction succeeded ({apt_total} packages).{N}")
            for pkg in apt_queue:
                checkpoint.mark_done(pkg)
                succeeded.append(pkg)
        else:
            print(f"  {Y}[~] Bulk failed (rc={bulk_rc}). Isolating culprits...{N}\n")
            for idx, pkg in enumerate(apt_queue, 1):
                ok, _ = _install_apt_package(pkg, env, idx, apt_total)
                (succeeded if ok else failed).append(pkg)

    # Phase 2: GitHub tools
    if github_queue:
        print(f"\n{Y}[*] PHASE 2 -- GITHUB SAFE INSTALL ({len(github_queue)} tools){N}\n")
        github_total = len(github_queue)
        for idx, tool in enumerate(github_queue, 1):
            ok, _ = _install_github_tool(tool, idx, github_total)
            (succeeded if ok else failed).append(tool)

    _cache.invalidate_many(all_packages)
    _selected_assets.difference_update(set(succeeded))
    _print_deploy_summary(succeeded, failed)
    input(f"\n{C}Press ENTER...{N}")


def resume_deploy(pending_packages: list) -> None:
    """
    Resume an interrupted batch. Reuses the existing checkpoint file.
    Called by main() when check_resume() returns a non-empty list.
    """
    if not pending_packages:
        return
    require_root("resume deploy")
    if not assert_dpkg_free():
        input(f"{C}Press ENTER...{N}")
        return
    if not _run_preflight():
        input(f"{C}Press ENTER to abort...{N}")
        return

    apt_queue, github_queue = _partition_queue(set(pending_packages))
    all_packages = apt_queue + github_queue

    clear_screen()
    banner(None, "CHECKPOINT RECOVERY ENGINE")
    print(f"\n{Y}[*] Resuming {len(all_packages)} pending package(s)...{N}\n")

    succeeded: list = []
    failed:    list = []
    env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}

    if apt_queue:
        print(f"{Y}[*] APT RECOVERY ({len(apt_queue)} packages){N}\n")
        for idx, pkg in enumerate(apt_queue, 1):
            ok, _ = _install_apt_package(pkg, env, idx, len(apt_queue))
            (succeeded if ok else failed).append(pkg)

    if github_queue:
        print(f"\n{Y}[*] GITHUB RECOVERY ({len(github_queue)} tools){N}\n")
        for idx, tool in enumerate(github_queue, 1):
            ok, _ = _install_github_tool(tool, idx, len(github_queue))
            (succeeded if ok else failed).append(tool)

    _cache.invalidate_many(all_packages)
    _selected_assets.difference_update(set(succeeded))
    _print_deploy_summary(succeeded, failed)
    input(f"\n{C}Press ENTER...{N}")


# ==============================================================================
# SECTION 24 - SINGLE-PACKAGE OPERATIONS
# ==============================================================================

def upgrade_asset(tool: str) -> None:
    require_root("upgrade")
    if not assert_dpkg_free() or not _run_preflight():
        input(f"{C}Press ENTER...{N}")
        return
    _cache.invalidate(tool)
    clear_screen()
    banner(None, f"UPGRADING: {tool}")
    target_path = f"/root/{tool}"

    if tool in GITHUB_MAP:
        ok, err = upgrade_python_tool_safe(
            tool, f"https://github.com/{GITHUB_MAP[tool]}.git"
        )
        if ok:
            print(f"\n{G}[+] {tool} SYNCED SUCCESSFULLY.{N}")
        else:
            error_registry.log("UPGRADE", tool, err, 1)
            print(f"\n{R}[-] UPGRADE FAILED.{N}")
    elif os.path.isdir(os.path.join(target_path, ".git")):
        original = os.getcwd()
        os.chdir(target_path)
        _, err, rc = run_cmd(["git", "pull"], timeout=60, capture=False)
        os.chdir(original)
        if rc != 0:
            error_registry.log("UPGRADE", tool, err, rc)
            print(f"\n{R}[-] GIT PULL FAILED (rc={rc}).{N}")
        else:
            print(f"\n{G}[+] {tool} UPDATED VIA GIT.{N}")
    else:
        env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
        _, err, rc = run_cmd(
            ["apt-get", "install", "--only-upgrade", "-y", tool],
            timeout=120, capture=False, env=env,
        )
        if rc != 0:
            error_registry.log("UPGRADE", tool, err, rc)
            print(f"\n{R}[-] APT UPGRADE FAILED (rc={rc}).{N}")
        else:
            print(f"\n{G}[+] {tool} SYNCED WITH APT.{N}")

    input(f"\n{C}Press ENTER...{N}")


def deploy_asset(tool: str) -> None:
    require_root("deploy")
    if not assert_dpkg_free() or not _run_preflight():
        input(f"{C}Press ENTER...{N}")
        return
    _cache.invalidate(tool)
    clear_screen()
    banner(None, f"DEPLOYING: {tool}")
    env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
    _, err, rc = run_cmd(
        ["apt-get", "install", "-y", tool],
        timeout=180, capture=False, env=env,
    )
    if rc != 0:
        error_registry.log("DEPLOY", tool, err, rc)
        print(f"\n{R}[-] DEPLOYMENT FAILED (rc={rc}).{N}")
    else:
        print(f"\n{G}[+] {tool} DEPLOYED SUCCESSFULLY.{N}")
    input(f"\n{C}Press ENTER...{N}")


def handle_selection(tool: str) -> None:
    safe = sanitize_package_name(tool)
    if safe is None:
        print(f"{R}[!] INVALID PACKAGE NAME: '{tool}'{N}")
        time.sleep(1.5)
        return
    status = get_pkg_status(safe)
    if status == 2:
        print(f"\n{Y}[!] {safe} IS OUTDATED.{N}")
        action = input(
            f"{C}>> [U] UPGRADE | [R] REINSTALL | [C] CANCEL: {N}"
        ).strip().lower()
        if action == "u":
            upgrade_asset(safe)
        elif action == "r":
            deploy_asset(safe)
    elif status == 1:
        print(f"\n{G}[*] {safe} IS AT THE LATEST REVISION.{N}")
        time.sleep(1.5)
    else:
        deploy_asset(safe)


# ==============================================================================
# SECTION 25 - NAVIGATOR (domain browser with multi-select)
# ==============================================================================

def navigator(domain_id: int, sys_total) -> None:
    """
    Paginated domain browser.
    Entering an ID toggles the package in _selected_assets.
    [D] triggers batch_deploy on the full selection queue.
    [E] opens the error log (shown only when registry is non-empty).
    """
    name       = DOMAIN_DEFS[domain_id]["name"]
    tools_list = load_domain(domain_id)
    limit, offset = 15, 0

    while True:
        clear_screen()
        banner(sys_total, name.upper())
        print(f"  {guardian.status_line()}\n")

        view = tools_list[offset: offset + limit]
        print(f"{B}-- DOMAIN: {G}{name.upper()} {B}| ASSETS: {G}{len(tools_list)} {B}--{N}")

        for i, t in enumerate(view):
            idx     = offset + i + 1
            badge   = status_badge(get_pkg_status(t))
            stag    = selected_tag(t)
            row_col = M if t in _selected_assets else W
            print(f" {G}[{idx:04d}]{N} {row_col}{t.ljust(35)}{N} {badge}{stag}")

        print(f"{C}--------------------------------------------------------------{N}")

        footer = f"{Y}[N]{W}ext  {Y}[B]{W}ack  {Y}[S]{W}earch"
        if _selected_assets:
            footer += f"  {M}[D]{W}eploy({len(_selected_assets)})"
        if not error_registry.is_empty:
            footer += f"  {R}[E]{W}rrors({error_registry.count})"
        footer += f"  {R}[M]{W}enu"
        print(f" {footer}")

        ans = input(f"{C}>> {N}").lower().strip()

        if ans == "m":
            break
        elif ans == "s":
            search_engine(tools_list, sys_total)
        elif ans == "d" and _selected_assets:
            batch_deploy(_selected_assets.copy())
        elif ans == "e" and not error_registry.is_empty:
            show_error_log()
        elif ans == "n" and offset + limit < len(tools_list):
            offset += limit
        elif ans == "b" and offset - limit >= 0:
            offset -= limit
        elif ans.isdigit():
            idx = int(ans)
            if offset < idx <= offset + len(view):
                pkg = tools_list[idx - 1]
                if pkg in _selected_assets:
                    _selected_assets.discard(pkg)
                    print(f"{DIM}[-] Deselected: {pkg}{N}")
                else:
                    _selected_assets.add(pkg)
                    print(f"{M}[+] Selected  : {pkg}{N}")
                time.sleep(0.4)


# ==============================================================================
# SECTION 26 - FULL SYSTEM UPGRADE
# ==============================================================================

def full_upgrade() -> None:
    """
    Run apt-get dist-upgrade with all safety wrappers.
    Clears SmartCache and domain cache afterwards to force fresh data.
    """
    require_root("full-upgrade")
    if not assert_dpkg_free() or not _run_preflight():
        input(f"{C}Press ENTER...{N}")
        return
    clear_screen()
    banner(None, "FULL-UPGRADE")
    print(f"{R}[!] WARNING: GLOBAL SYSTEM UPGRADE.{N}")
    if input(f"{Y}PROCEED? (y/n): {N}").lower() != "y":
        return
    print(f"\n{Y}[*] Running apt-get update...{N}")
    run_cmd(["apt-get", "update"], timeout=120, capture=False)
    print(f"\n{Y}[*] Running apt-get dist-upgrade...{N}")
    run_cmd(["apt-get", "dist-upgrade", "-y"], timeout=600, capture=False)
    _cache.clear()
    _domain_cache.clear()
    input(f"\n{G}[+] SYSTEM UPDATED. Press ENTER...{N}")


# ==============================================================================
# SECTION 27 - ERROR LOG VIEWER
# ==============================================================================

def show_error_log() -> None:
    """Display all ErrorRegistry records. Only reachable when registry is non-empty."""
    while True:
        clear_screen()
        banner(None, "SYSTEM ERROR REGISTRY")
        records = error_registry.all_records()
        if not records:
            print(f"{G}[*] NO ERRORS RECORDED IN THIS SESSION.{N}")
            input(f"\n{C}Press ENTER...{N}")
            return
        for i, rec in enumerate(records, 1):
            print(f"{R}--------------------------------------------------------------{N}")
            print(f"  {Y}[{i:02d}]{N}  {rec.timestamp}  {R}{rec.operation}{N}  ->  {W}{rec.package}{N}")
            print(f"  {DIM}RC  : {rec.returncode}{N}")
            print(f"  {DIM}ERR : {rec.stderr[:120].strip()}{N}")
            print(f"  {C}FIX : {rec.suggested}{N}")
        print(f"{R}--------------------------------------------------------------{N}")
        print(f"\n  {Y}[C]{W} CLEAR LOG   {R}[M]{W} BACK")
        ans = input(f"{C}>> LOG: {N}").strip().lower()
        if ans == "c":
            error_registry.clear()
            print(f"{G}[+] ERROR LOG CLEARED.{N}")
            time.sleep(1)
            return
        elif ans == "m":
            return


# ==============================================================================
# SECTION 28 - MAIN ENTRY POINT
# ==============================================================================

def get_system_total() -> int:
    """Query apt-cache stats for the total known package count."""
    stdout, _, _ = run_cmd(["apt-cache", "stats"], timeout=10)
    match = re.search(r"Total package names:\s+(\d+)", stdout)
    return int(match.group(1)) if match else 0


def main() -> None:
    """
    Application entry point.

    Startup sequence:
      1. Initialize SystemGuardian and ErrorRegistry singletons.
      2. Check for an interrupted checkpoint session (check_resume).
      3. Run resume_deploy if the user confirms.
      4. Fetch total package count for the banner.
      5. Enter the main menu loop.

    Root is NOT required at startup. It is enforced only by write
    operations (batch_deploy, resume_deploy, full_upgrade).
    """
    global guardian, error_registry

    # --- Object initialization (explicit, before any loop) ---
    guardian       = SystemGuardian()
    error_registry = ErrorRegistry()

    # --- Boot-time checkpoint recovery ---
    pending = checkpoint.check_resume()
    if pending:
        resume_deploy(pending)

    # --- Total package count (lazy domains remain as ??? until selected) ---
    sys_total = get_system_total()

    # --- Main menu loop ---
    while True:
        clear_screen()
        banner(sys_total)
        print(f"  {guardian.status_line()}\n")

        for k in range(1, 21):
            name  = DOMAIN_DEFS[k]["name"]
            count = get_domain_count(k)
            print(f"  {G}[{k:02d}]{W} {name.ljust(35)} {B}({G}{count} PKGS{B})")

        print(f"{C}--------------------------------------------------------------{N}")

        footer_items = [
            f"{Y}[S]{W} SEARCH",
            f"{Y}[U]{W} FULL UPGRADE",
        ]
        if _selected_assets:
            footer_items.append(f"{M}[D]{W} DEPLOY ({len(_selected_assets)} SEL)")
        if not error_registry.is_empty:
            footer_items.append(f"{R}[E]{W} ERRORS ({error_registry.count})")
        footer_items.append(f"{R}[Q]{W} QUIT")
        print("  " + "   ".join(footer_items))

        choice = input(f"\n{C}>> OS_MASTER: {N}").lower().strip()

        if choice == "q":
            if _selected_assets:
                confirm = input(
                    f"\n{Y}[!] {len(_selected_assets)} package(s) still selected. "
                    f"Quit anyway? (y/n): {N}"
                ).strip().lower()
                if confirm not in ("y", "yes"):
                    continue
            break
        elif choice == "s":
            search_engine(sys_total=sys_total)
        elif choice == "u":
            full_upgrade()
            sys_total = get_system_total()
        elif choice == "d" and _selected_assets:
            batch_deploy(_selected_assets.copy())
        elif choice == "e" and not error_registry.is_empty:
            show_error_log()
        elif choice.isdigit():
            ch = int(choice)
            if ch in DOMAIN_DEFS:
                navigator(ch, sys_total)

    if not checkpoint.has_incomplete_session():
        checkpoint.close_session()

    print(f"\n{DIM}[*] OMNI-MASTER session ended.{N}\n")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{R}[!] TERMINATED BY USER.{N}")
        sys.exit(0)

