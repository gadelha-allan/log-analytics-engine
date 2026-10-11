from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from src import publication
from src.processor import process_logs


def _snapshot(*directories: Path) -> dict[Path, tuple[bytes, int]]:
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in directories
        for path in directory.rglob("*")
        if path.is_file()
    }


def _backups(directory: Path) -> list[Path]:
    return sorted(directory.parent.glob(f".{directory.name}.backup-*"))


def _assert_no_leftovers(*directories: Path) -> None:
    for directory in directories:
        assert not _backups(directory)
        assert not list(directory.parent.glob(f".{directory.name}.staging-*"))


@pytest.mark.parametrize("rejected", [False, True])
@pytest.mark.parametrize("after_rename", [False, True])
@pytest.mark.parametrize(
    "failure", ["backup_lake", "backup_quarantine", "lake", "quarantine"]
)
def test_falha_na_substituicao_restaura_parquets_anteriores(
    rejected: bool,
    after_rename: bool,
    failure: str,
    tmp_path: Path,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output, quarantine = temp_output_dirs
    previous = process_logs(mock_log_file, output, quarantine)
    before = _snapshot(output, quarantine)
    replacement = tmp_path / "replacement.log"
    replacement.write_text(
        '10.0.0.1 - - [28/Jul/2026:14:32:10 +0000] "GET /new HTTP/1.1" 200 3000000000\n'
        + ('  nova linha, "invalida"  \n' if rejected else "")
    )
    rename = Path.rename
    interrupted = False

    def fail_once(source: Path, target: Path) -> Path:
        nonlocal interrupted
        target = Path(target)
        should_fail = {
            "backup_lake": source == output,
            "backup_quarantine": source == quarantine,
            "lake": target == output and ".staging-" in source.name,
            "quarantine": target == quarantine and ".staging-" in source.name,
        }[failure]
        if should_fail and not interrupted:
            interrupted = True
            if after_rename:
                rename(source, target)
            raise OSError(f"substituicao interrompida: {failure}")
        return rename(source, target)

    monkeypatch.setattr(Path, "rename", fail_once)
    with (
        caplog.at_level(logging.INFO),
        pytest.raises(OSError, match="substituicao interrompida"),
    ):
        process_logs(replacement, output, quarantine, full_refresh=True)

    assert interrupted
    assert _snapshot(output, quarantine) == before
    assert (
        pl.read_parquet(list(output.rglob("*.parquet"))).to_dicts()
        == previous["valid"].to_dicts()
    )
    assert (
        pl.read_parquet(quarantine / "quarantine.parquet").to_dicts()
        == previous["quarantine"].to_dicts()
    )
    assert "Preparacao validada" in caplog.text
    assert "Iniciando recuperacao" in caplog.text
    assert "saida anterior restaurada" in caplog.text
    assert "Publicacao concluida" not in caplog.text
    _assert_no_leftovers(output, quarantine)


@pytest.mark.parametrize("rejected", [False, True])
def test_backups_retidos_ate_publicacao_dos_dois_destinos(
    rejected: bool,
    tmp_path: Path,
    mock_log_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output = tmp_path / "lake-volume" / "logs"
    quarantine = tmp_path / "quarantine-volume" / "rejected"
    process_logs(mock_log_file, output, quarantine)
    before = _snapshot(output, quarantine)
    replacement = tmp_path / "replacement.log"
    valid_line = (
        '10.0.0.1 - - [28/Jul/2026:14:32:10 +0000] "GET /new HTTP/1.1" 200 3000000000'
    )
    rejected_line = '  nova linha, "invalida"  '
    replacement.write_text(
        valid_line + "\n" + (rejected_line + "\n" if rejected else "")
    )
    rename = Path.rename
    installed = []

    def check_backups(source: Path, target: Path) -> Path:
        target = Path(target)
        if ".staging-" in source.name:
            for destination in (output, quarantine):
                backup = _backups(destination)[0] / "previous"
                assert {
                    destination / path.relative_to(backup): contents
                    for path, contents in _snapshot(backup).items()
                } == {
                    path: contents
                    for path, contents in before.items()
                    if path.is_relative_to(destination)
                }
            installed.append(target)
        return rename(source, target)

    monkeypatch.setattr(Path, "rename", check_backups)
    with caplog.at_level(logging.INFO):
        processed = process_logs(replacement, output, quarantine, full_refresh=True)

    assert installed == [output, quarantine]
    assert pl.read_parquet(list(output.rglob("*.parquet")))["raw"].to_list() == [
        valid_line
    ]
    assert processed["valid"]["size"].to_list() == [3_000_000_000]
    assert not (output / "dt_partition=2026-07-27").exists()
    if rejected:
        assert pl.read_parquet(quarantine / "quarantine.parquet").select(
            "raw", "rejection_reason"
        ).rows() == [(rejected_line, "regex_mismatch")]
    else:
        assert processed["quarantine"] is None
        assert list(quarantine.iterdir()) == []
    messages = caplog.text
    assert messages.index("Preparacao validada") < messages.index("Preservando")
    assert messages.index("Preservando") < messages.index("Publicando")
    assert messages.index("Publicando") < messages.index("Publicacao concluida")
    assert "Iniciando recuperacao" not in messages
    _assert_no_leftovers(output, quarantine)


@pytest.mark.parametrize("existing", ["none", "lake", "quarantine", "empty"])
@pytest.mark.parametrize("failure_index", [0, 1])
def test_recuperacao_preserva_destinos_ausentes_ou_vazios(
    existing: str, failure_index: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    targets = (tmp_path / "lake", tmp_path / "quarantine")
    stages = (tmp_path / "stage-lake", tmp_path / "stage-quarantine")
    for index, destination in enumerate(targets):
        if existing == "empty" or existing == ("lake", "quarantine")[index]:
            destination.mkdir()
            if existing != "empty":
                (destination / "old.parquet").write_bytes(b"previous version")
    for stage in stages:
        stage.mkdir()
        (stage / "new.parquet").write_bytes(b"prepared version")
    before = _snapshot(*targets)
    existed = [destination.exists() for destination in targets]
    rename = Path.rename

    def fail_install(source: Path, target: Path) -> Path:
        if source == stages[failure_index]:
            raise OSError("install failed")
        return rename(source, target)

    monkeypatch.setattr(Path, "rename", fail_install)
    with pytest.raises(OSError, match="install failed"):
        publication.publish_outputs(tuple(zip(stages, targets, strict=True)))

    assert _snapshot(*targets) == before
    assert [destination.exists() for destination in targets] == existed
    for stage in stages:
        assert (stage / "new.parquet").read_bytes() == b"prepared version"
    _assert_no_leftovers(*targets)


def test_interrupcao_durante_publicacao_restaura_par_antes_de_propagar(
    tmp_path: Path,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output, quarantine = temp_output_dirs
    process_logs(mock_log_file, output, quarantine)
    before = _snapshot(output, quarantine)
    rename = Path.rename

    def interrupt(source: Path, target: Path) -> Path:
        if Path(target) == quarantine and ".staging-" in source.name:
            raise KeyboardInterrupt
        return rename(source, target)

    monkeypatch.setattr(Path, "rename", interrupt)
    with pytest.raises(KeyboardInterrupt):
        process_logs(mock_log_file, output, quarantine, full_refresh=True)

    assert _snapshot(output, quarantine) == before
    _assert_no_leftovers(output, quarantine)


@pytest.mark.parametrize("recovery_failure", ["remove_new", "restore_old"])
def test_falha_na_recuperacao_preserva_backup_e_informa_caminho(
    recovery_failure: str,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output, quarantine = temp_output_dirs
    process_logs(mock_log_file, output, quarantine)
    before = _snapshot(output, quarantine)
    rename = Path.rename

    def fail_publication_and_recovery(source: Path, target: Path) -> Path:
        target = Path(target)
        if target == quarantine and ".staging-" in source.name:
            raise OSError("publication failed")
        if (
            recovery_failure == "remove_new"
            and source == output
            and ".staging-" in target.name
        ):
            raise OSError("cannot withdraw new output")
        if source.name == "previous" and target == output:
            raise OSError("cannot restore lake")
        return rename(source, target)

    monkeypatch.setattr(Path, "rename", fail_publication_and_recovery)
    with pytest.raises(
        publication.PublicationRecoveryError, match="Backups preservados"
    ) as caught:
        process_logs(mock_log_file, output, quarantine, full_refresh=True)

    assert isinstance(caught.value.__cause__, OSError)
    assert str(caught.value.__cause__) == "publication failed"
    backup_root = _backups(output)[0]
    assert str(backup_root) in str(caught.value)
    backup = backup_root / "previous"
    assert {
        output / path.relative_to(backup): contents
        for path, contents in _snapshot(backup).items()
    } == {
        path: contents
        for path, contents in before.items()
        if path.is_relative_to(output)
    }
    assert _snapshot(quarantine) == {
        path: contents
        for path, contents in before.items()
        if path.is_relative_to(quarantine)
    }
    assert "Falha ao restaurar" in caplog.text
    assert "Recuperacao concluida" not in caplog.text


def test_falha_ao_limpar_backup_nao_desfaz_publicacao_concluida(
    tmp_path: Path,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output, quarantine = temp_output_dirs
    process_logs(mock_log_file, output, quarantine)
    replacement = tmp_path / "replacement.log"
    valid_line = '10.0.0.1 - - [28/Jul/2026:14:32:10 +0000] "GET /new HTTP/1.1" 200 100'
    replacement.write_text(valid_line + "\n")
    rmtree = publication.shutil.rmtree

    def fail_backup_cleanup(directory, *args, **kwargs):
        if Path(directory).name.startswith(f".{output.name}.backup-"):
            assert pl.read_parquet(list(output.rglob("*.parquet")))[
                "raw"
            ].to_list() == [valid_line]
            assert list(quarantine.iterdir()) == []
            raise PermissionError("backup locked")
        return rmtree(directory, *args, **kwargs)

    monkeypatch.setattr(publication.shutil, "rmtree", fail_backup_cleanup)
    with caplog.at_level(logging.INFO):
        processed = process_logs(replacement, output, quarantine, full_refresh=True)

    assert processed["valid"]["raw"].to_list() == [valid_line]
    assert _backups(output)
    assert not _backups(quarantine)
    assert "Nao foi possivel remover o backup" in caplog.text
    assert "backup locked" in caplog.text
    assert "Publicacao concluida" in caplog.text
