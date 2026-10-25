import posixpath
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import AbstractSet, Any, NamedTuple  # noqa: UP035

import lkml
from dagster import AssetSpec
from dagster._core.utils import toposort_flatten
from dagster_shared.yaml_utils import safe_load_yaml

from dagster_looker.lkml.dagster_looker_lkml_translator import (
    DagsterLookerLkmlTranslator,
    LookMLStructureType,
)

IMPORTED_PROJECTS_DIR = "imported_projects"


def deep_merge_objs(onto_obj: Any, from_obj: Any) -> Any:
    """Deep merge, which on dictionaries merges keys, and on lists appends elements.
    If a value is present in both onto_obj and from_obj, the value from from_obj is taken.
    For lists, entries are deduplicated by "name" key, resolving conflicts in favor of from_obj.
    """
    # We use ... as a sentinel value for "no value", since we want to distinguish between
    # None and "no value" - None values in the from_obj should override onto_obj values,
    # but if from_obj has no value, we should keep the onto_obj value.
    if isinstance(onto_obj, dict):
        if from_obj in (None, ...):
            return onto_obj
        if not isinstance(from_obj, dict):
            raise Exception("Cannot merge a dictionary with a non-dictionary object")

        result = {
            key: deep_merge_objs(onto_obj.get(key, ...), from_obj.get(key, ...))
            for key in onto_obj.keys() | from_obj.keys()
        }

        return result
    elif isinstance(onto_obj, list):
        if from_obj in (None, ...):
            return onto_obj
        if not isinstance(from_obj, list):
            raise Exception("Cannot merge a list with a non-list object")

        from_obj_names = [
            obj["name"] for obj in from_obj if isinstance(obj, dict) and "name" in obj
        ]
        result = [
            obj
            for obj in onto_obj
            if not isinstance(obj, dict) or obj.get("name") not in from_obj_names
        ] + from_obj

        return result
    else:
        if from_obj is not ...:
            return from_obj
        elif onto_obj is not ...:
            return onto_obj
        else:
            return None


class LookMLStructure(NamedTuple):
    path: Path
    structure_type: LookMLStructureType
    props: Mapping[str, Any]


def postprocess_loaded_structures(
    structs: list[LookMLStructure],
) -> list[LookMLStructure]:
    """Postprocesses LookML structs to resolve refinements and extends.

    https://cloud.google.com/looker/docs/lookml-refinements
    https://cloud.google.com/looker/docs/reusing-code-with-extends
    """
    refined_objs_by_name = {}
    for path, typ, obj_props in structs:
        obj_name = obj_props["name"]
        if not obj_name.startswith("+"):
            refined_objs_by_name[obj_name] = (path, typ, obj_props)

    # Process refinements
    for path, typ, obj_props in structs:
        obj_name = obj_props["name"]
        if obj_name.startswith("+") and obj_name[1:] in refined_objs_by_name:
            refined_objs_by_name[obj_name[1:]] = (
                path,
                typ,
                deep_merge_objs(
                    deep_merge_objs(refined_objs_by_name[obj_name[1:]][2], obj_props),
                    {"name": obj_name[1:]},
                ),
            )

    # Process extends in topological order
    extension_graph: Mapping[str, AbstractSet[str]] = {
        obj_props["name"]: set(obj_props.get("extends__all", [[]])[0])
        for _, _, obj_props in refined_objs_by_name.values()
    }
    evaluation_order = toposort_flatten(extension_graph)

    refined_extended_objs_by_name = {}
    for obj_name in evaluation_order:
        base_obj = refined_objs_by_name[obj_name]
        _, _, obj_props = base_obj
        extensions = obj_props.get("extends__all", [[]])[0]

        output_props = obj_props
        # Merge extensions in reverse order, so that the last extension takes precedence
        # https://cloud.google.com/looker/docs/reusing-code-with-extends#extending_more_than_one_object_at_the_same_time
        for extension in reversed(extensions):
            extension_obj = refined_extended_objs_by_name[extension]
            _, _, extension_props = extension_obj
            output_props = deep_merge_objs(extension_props, output_props)

        refined_extended_objs_by_name[obj_name] = LookMLStructure(
            base_obj[0], base_obj[1], output_props
        )

    return list(refined_extended_objs_by_name.values())


def build_looker_dashboard_specs(
    project_dir: Path,
    dagster_looker_translator: DagsterLookerLkmlTranslator,
) -> Sequence[AssetSpec]:
    looker_dashboard_specs: list[AssetSpec] = []

    # https://cloud.google.com/looker/docs/reference/param-lookml-dashboard
    for lookml_dashboard_path in project_dir.rglob("*.dashboard.lookml"):
        for lookml_dashboard_props in safe_load_yaml(lookml_dashboard_path.read_bytes()):
            lookml_dashboard = LookMLStructure(
                path=lookml_dashboard_path, structure_type="dashboard", props=lookml_dashboard_props
            )

            looker_dashboard_specs.append(
                dagster_looker_translator.get_asset_spec(lookml_dashboard)
            )

    return looker_dashboard_specs


def build_looker_explore_specs(
    project_dir: Path,
    dagster_looker_translator: DagsterLookerLkmlTranslator,
) -> Sequence[AssetSpec]:

    explores = []
    # https://cloud.google.com/looker/docs/reference/param-explore
    for lookml_model_path in project_dir.rglob("*.model.lkml"):
        for lookml_explore_props in lkml.load(lookml_model_path.read_text()).get("explores", []):
            lookml_explore = LookMLStructure(
                path=lookml_model_path, structure_type="explore", props=lookml_explore_props
            )
            explores.append(lookml_explore)

    explores_postprocessed = postprocess_loaded_structures(explores)
    looker_explore_specs: list[AssetSpec] = [
        dagster_looker_translator.get_asset_spec(lookml_explore)
        for lookml_explore in explores_postprocessed
    ]

    return looker_explore_specs


def _get_project_root(project_dir: Path, lookml_path: Path) -> Path:
    """Returns the root of the project a file belongs to. Files of an imported project are
    checked out under `imported_projects/<project_name>/`.
    """
    relative_parts = lookml_path.relative_to(project_dir).parts
    if len(relative_parts) > 2 and relative_parts[0] == IMPORTED_PROJECTS_DIR:
        return project_dir.joinpath(*relative_parts[:2])

    return project_dir


def _build_include_pattern_regex(pattern: str) -> re.Pattern[str]:
    """Translates an include pattern, relative to a project root, into a regex. `*` matches
    within one directory and a `**` directory matches any number of directories.
    """
    segments = pattern.split("/")
    regex = ""
    for i, segment in enumerate(segments):
        if segment == "**":
            regex += r"(?:[^/]+/)*"
        else:
            regex += re.escape(segment).replace(r"\*", "[^/]*")
            if i < len(segments) - 1:
                regex += "/"

    return re.compile(regex)


def _resolve_include(
    project_dir: Path,
    lookml_paths: Sequence[Path],
    including_path: Path,
    include: str,
) -> Sequence[Path]:
    """Returns the files an `include` statement matches, in path order.

    https://cloud.google.com/looker/docs/reference/param-model-include
    """
    if include.startswith("//"):
        imported_project_name, _, pattern = include[2:].partition("/")
        root = project_dir / IMPORTED_PROJECTS_DIR / imported_project_name
    else:
        root = _get_project_root(project_dir, including_path)
        if include.startswith("/"):
            pattern = include[1:]
        else:
            pattern = posixpath.normpath(
                (including_path.parent.relative_to(root) / include).as_posix()
            )

    # The file extension can be left out of an include, for example `/views/*.view`.
    if not pattern.endswith((".lkml", ".lookml")):
        pattern += ".lkml"
    pattern_regex = _build_include_pattern_regex(pattern)

    resolved_paths = []
    for lookml_path in lookml_paths:
        if not lookml_path.is_relative_to(root):
            continue
        relative_path = lookml_path.relative_to(root).as_posix()
        # A pattern of the current project never matches the files of an imported project.
        if root == project_dir and relative_path.startswith(f"{IMPORTED_PROJECTS_DIR}/"):
            continue
        if pattern_regex.fullmatch(relative_path):
            resolved_paths.append(lookml_path)

    return resolved_paths


def _load_lookml_files_in_include_order(project_dir: Path) -> Mapping[Path, Mapping[str, Any]]:
    """Loads the LookML files that the project's models include, in the order Looker reads
    them: each included file's own includes come before the file, a file is read once at the
    position it is first included, and each model file comes after the files it includes.
    Model files of imported projects are not models of this project, so they are skipped.

    https://cloud.google.com/looker/docs/lookml-refinements#refinements_are_applied_in_order
    """
    lookml_paths = sorted(
        lookml_path
        for lookml_path in project_dir.rglob("*.lkml")
        if not lookml_path.name.endswith(".dashboard.lkml")
    )

    visited_paths: set[Path] = set()
    loaded_lookml_by_path: dict[Path, Mapping[str, Any]] = {}

    def visit(lookml_path: Path) -> None:
        if lookml_path in visited_paths:
            return
        visited_paths.add(lookml_path)

        loaded_lookml = lkml.load(lookml_path.read_text(encoding="utf-8"))
        for include in loaded_lookml.get("includes", []):
            for included_path in _resolve_include(project_dir, lookml_paths, lookml_path, include):
                visit(included_path)

        loaded_lookml_by_path[lookml_path] = loaded_lookml

    for lookml_path in lookml_paths:
        if (
            lookml_path.name.endswith(".model.lkml")
            and _get_project_root(project_dir, lookml_path) == project_dir
        ):
            visit(lookml_path)

    return loaded_lookml_by_path


def build_looker_view_specs(
    project_dir: Path,
    dagster_looker_translator: DagsterLookerLkmlTranslator,
) -> Sequence[AssetSpec]:

    # Views and refinements are read in include order, so that refinements apply in the order
    # Looker applies them. They can be declared in any included LookML file, not only in
    # `.view.lkml` files. View files that no model includes are still read, in path order,
    # before the included files, so that what the models include takes precedence.
    #
    # https://cloud.google.com/looker/docs/reference/param-view
    # https://cloud.google.com/looker/docs/lookml-refinements
    included_lookml_by_path = _load_lookml_files_in_include_order(project_dir)
    not_included_view_paths = sorted(
        set(project_dir.rglob("*.view.lkml")) - included_lookml_by_path.keys()
    )
    lookml_by_path = {
        **{
            lookml_view_path: lkml.load(lookml_view_path.read_text(encoding="utf-8"))
            for lookml_view_path in not_included_view_paths
        },
        **included_lookml_by_path,
    }

    views: list[LookMLStructure] = []
    for lookml_path, loaded_lookml in lookml_by_path.items():
        views.extend(
            LookMLStructure(lookml_path, "view", lookml_view_props)
            for lookml_view_props in loaded_lookml.get("views", [])
        )
    views_postprocessed = postprocess_loaded_structures(views)

    looker_view_specs: list[AssetSpec] = [
        dagster_looker_translator.get_asset_spec(lookml_view) for lookml_view in views_postprocessed
    ]

    return looker_view_specs
