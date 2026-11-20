from dataclasses import dataclass
from .codec import digest, dumps, field, loads, name, scalar_token
from .errors import Conflict, InvalidDocument, NotFound
from .indexes import refresh_document
from .mvcc import latest_row, visible_rows
from .schema import normalize_schema, validate_document


@dataclass(frozen=True)
class MigrationPlan:
    """Read-only rehearsal outcome bound to one database, revision and catalog snapshot."""
    collection: str
    database_id: str
    revision: int
    catalog_epoch: int
    generation: int
    schema: dict | None
    indexes: tuple
    documents: int
    incompatible: tuple
    conflicts: tuple
    blockers: tuple
    remedy: tuple
    executable: bool
    executable_after_remedy: bool


@dataclass(frozen=True)
class MigrationResult:
    revision: int
    generation: int
    indexes: tuple
    removed: tuple


def index_specs(indexes):
    specs = []
    seen = set()
    for spec in indexes:
        if not isinstance(spec, dict) or set(spec) - {"name", "field", "unique"} or "name" not in spec or "field" not in spec:
            raise InvalidDocument("index spec requires name and field and accepts unique")
        unique = spec.get("unique", False)
        if not isinstance(unique, bool):
            raise InvalidDocument("unique must be boolean")
        index = name(spec["name"])
        path = spec["field"]
        field({}, path)
        if index in seen:
            raise InvalidDocument("duplicate index in request: " + index)
        seen.add(index)
        specs.append({"name": index, "field": path, "unique": unique})
    return specs


def schema_failures(rows, normalized):
    failures = []
    if normalized is None:
        return failures
    for row in rows:
        try:
            validate_document(loads(row["body"]), normalized)
        except InvalidDocument as exc:
            failures.append({"key": row["key"], "revision": row["rev"], "reason": str(exc)})
    return failures


def unique_conflicts(rows, specs):
    conflicts = []
    for spec in specs:
        if not spec["unique"]:
            continue
        groups = {}
        for row in rows:
            token = scalar_token(field(loads(row["body"]), spec["field"]))
            if token is not None:
                groups.setdefault(token, []).append(row["key"])
        for token in sorted(groups):
            if len(groups[token]) > 1:
                conflicts.append({"index": spec["name"], "field": spec["field"], "token": token, "keys": groups[token]})
    return conflicts


def remedy_keys(incompatible, conflicts):
    dropped = {item["key"] for item in incompatible}
    kept = set()
    for group in conflicts:
        candidates = [key for key in group["keys"] if key not in dropped]
        if candidates:
            kept.add(candidates[0])
    removed = set(dropped)
    for group in conflicts:
        removed.update(key for key in group["keys"] if key not in kept)
    return removed


class Migration:
    """Rehearse schema/index tightening on a fixed snapshot, then apply it under a fresh check."""
    def __init__(self, store):
        self.store = store

    def rehearse(self, collection, schema=None, indexes=()):
        name(collection)
        normalized = normalize_schema(schema) if schema is not None else None
        specs = index_specs(indexes)
        if normalized is None and not specs:
            raise InvalidDocument("rehearsal needs a schema or at least one index")
        with self.store.read() as connection:
            row = connection.execute("SELECT * FROM collections WHERE name=?", (collection,)).fetchone()
            if row is None:
                raise NotFound("unknown collection: " + collection)
            head = int(self.store.get("head"))
            rows = visible_rows(connection, collection, head)
            blockers = ["index already exists: " + spec["name"] for spec in specs
                        if connection.execute("SELECT 1 FROM index_defs WHERE collection=? AND name=?",
                                              (collection, spec["name"])).fetchone()]
            incompatible = schema_failures(rows, normalized)
            conflicts = unique_conflicts(rows, specs)
            removed = remedy_keys(incompatible, conflicts)
            revisions = {item["key"]: item["rev"] for item in rows}
            remaining = [item for item in rows if item["key"] not in removed]
            cleaned = not schema_failures(remaining, normalized) and not unique_conflicts(remaining, specs)
            return MigrationPlan(collection=collection, database_id=self.store.get("database_id"), revision=head,
                                 catalog_epoch=int(self.store.get("catalog_epoch")), generation=row["generation"],
                                 schema=normalized, indexes=tuple(specs), documents=len(rows),
                                 incompatible=tuple(incompatible), conflicts=tuple(conflicts), blockers=tuple(blockers),
                                 remedy=tuple({"key": key, "expected": revisions[key]} for key in sorted(removed)),
                                 executable=not blockers and not incompatible and not conflicts,
                                 executable_after_remedy=not blockers and cleaned)

    def apply(self, plan, remedy=False):
        if not isinstance(plan, MigrationPlan):
            raise InvalidDocument("apply expects a MigrationPlan from rehearse")
        if not isinstance(remedy, bool):
            raise InvalidDocument("remedy must be boolean")
        normalized = normalize_schema(plan.schema) if plan.schema is not None else None
        specs = index_specs(plan.indexes)
        if normalized is None and not specs:
            raise InvalidDocument("plan contains no migration")
        with self.store.atomic(writable=True) as connection:
            if plan.database_id != self.store.get("database_id"):
                raise Conflict("migration plan belongs to a different database")
            row = connection.execute("SELECT * FROM collections WHERE name=?", (plan.collection,)).fetchone()
            if row is None:
                raise NotFound("unknown collection: " + plan.collection)
            if row["generation"] != plan.generation:
                raise Conflict("schema generation changed since rehearsal")
            epoch = int(self.store.get("catalog_epoch"))
            if epoch != plan.catalog_epoch:
                raise Conflict("catalog changed since rehearsal")
            head = int(self.store.get("head"))
            if head != plan.revision:
                raise Conflict("documents changed since rehearsal")
            if plan.blockers:
                raise Conflict("rehearsal reported blocking findings: " + "; ".join(plan.blockers))
            if not plan.executable and not remedy:
                raise Conflict("rehearsal findings require remedy or cleanup first")
            if remedy and not plan.executable_after_remedy:
                raise Conflict("suggested remedy cannot make this plan executable")
            removed, revision = [], head
            if remedy and plan.remedy:
                revision = head + 1
                operations = []
                for item in plan.remedy:
                    old = latest_row(connection, plan.collection, item["key"], head)
                    current = old["rev"] if old is not None else 0
                    if current != item["expected"]:
                        raise Conflict(f"revision conflict for {plan.collection}/{item['key']}: "
                                       f"expected {item['expected']}, found {current}")
                    connection.execute("INSERT INTO versions VALUES (?,?,?,?,?)", (plan.collection, item["key"], revision, None, None))
                    refresh_document(connection, plan.collection, item["key"], None)
                    operations.append({"collection": plan.collection, "key": item["key"], "document": None,
                                       "expected": item["expected"], "expires_at": None})
                    removed.append(item["key"])
                connection.execute("INSERT INTO commits VALUES (?,?,?,?)",
                                   (revision, dumps({"revision": revision, "operations": operations}), None, digest(operations)))
                self.store.set("head", revision)
            rows = visible_rows(connection, plan.collection, revision)
            failures = schema_failures(rows, normalized)
            if failures:
                raise Conflict("fresh schema evaluation failed for keys: " + ",".join(item["key"] for item in failures))
            collisions = unique_conflicts(rows, specs)
            if collisions:
                raise Conflict("fresh unique evaluation failed for keys: "
                               + ",".join(sorted({key for item in collisions for key in item["keys"]})))
            generation = plan.generation
            if normalized is not None:
                generation += 1
                connection.execute("UPDATE collections SET schema=?,generation=? WHERE name=?",
                                   (dumps(normalized), generation, plan.collection))
            installed = []
            for spec in specs:
                if connection.execute("SELECT 1 FROM index_defs WHERE collection=? AND name=?",
                                      (plan.collection, spec["name"])).fetchone():
                    raise Conflict("index already exists: " + spec["name"])
                connection.execute("INSERT INTO index_defs VALUES (?,?,?,?)",
                                   (plan.collection, spec["name"], spec["field"], int(spec["unique"])))
                seen = set()
                for item in rows:
                    token = scalar_token(field(loads(item["body"]), spec["field"]))
                    if token is None:
                        continue
                    if spec["unique"]:
                        if token in seen:
                            raise Conflict("existing documents violate unique index")
                        seen.add(token)
                    connection.execute("INSERT INTO index_values VALUES (?,?,?,?)", (plan.collection, spec["name"], token, item["key"]))
                installed.append(spec["name"])
            self.store.set("catalog_epoch", epoch + 1)
        return MigrationResult(revision, generation, tuple(installed), tuple(removed))
