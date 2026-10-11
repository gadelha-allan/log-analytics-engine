from __future__ import annotations

import logging
import shutil
from pathlib import Path
from tempfile import mkdtemp

logger = logging.getLogger(__name__)


class PublicationRecoveryError(RuntimeError):
    """Recovery failed; retained backups need manual restoration."""


def _remove_backups(backup_roots: list[Path]) -> None:
    for directory in backup_roots:
        try:
            shutil.rmtree(directory)
        except OSError as exc:
            logger.warning("Nao foi possivel remover o backup %s: %s", directory, exc)


def publish_outputs(outputs: tuple[tuple[Path, Path], ...]) -> None:
    """Replace prepared directories for one local writer, with rollback on failure."""
    backups: dict[Path, Path] = {}
    backup_roots: list[Path] = []
    installed: list[tuple[Path, Path]] = []
    try:
        for _, destination in outputs:
            if destination.exists():
                backup_root = Path(
                    mkdtemp(
                        prefix=f".{destination.name}.backup-", dir=destination.parent
                    )
                )
                backup_roots.append(backup_root)
                backup = backup_root / "previous"
                backups[destination] = backup
                logger.info("Preservando %s no backup %s", destination, backup)
                destination.rename(backup)

        for prepared, destination in outputs:
            installed.append((prepared, destination))
            logger.info("Publicando %s em %s", prepared, destination)
            prepared.rename(destination)
    except BaseException as exc:
        # Ctrl-C during replacement must also restore the previous pair.
        logger.error("Falha na publicacao: %s. Iniciando recuperacao.", exc)
        recovery_errors: list[str] = []
        for prepared, destination in reversed(installed):
            # A rename can complete just before an interruption is delivered.
            if prepared.exists():
                continue
            try:
                destination.rename(prepared)
            except OSError as recovery_exc:
                recovery_errors.append(f"{destination}: {recovery_exc}")
                logger.error("Falha ao retirar %s: %s", destination, recovery_exc)

        for destination, backup in reversed(backups.items()):
            if not backup.exists():
                continue
            try:
                logger.info("Restaurando %s de %s", destination, backup)
                backup.rename(destination)
            except OSError as recovery_exc:
                recovery_errors.append(f"{destination}: {recovery_exc}")
                logger.error("Falha ao restaurar %s: %s", destination, recovery_exc)

        if recovery_errors:
            # Backups cannot use automatic cleanup when restoration may fail.
            retained = ", ".join(str(directory) for directory in backup_roots)
            raise PublicationRecoveryError(
                f"Recuperacao incompleta. Backups preservados em {retained}. "
                + "; ".join(recovery_errors)
            ) from exc

        _remove_backups(backup_roots)
        logger.info("Recuperacao concluida; saida anterior restaurada.")
        raise

    logger.info(
        "Publicacao concluida para %s", ", ".join(str(dst) for _, dst in outputs)
    )
    _remove_backups(backup_roots)
