from __future__ import annotations
from collections import defaultdict
from typing import Any
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.responses import JSONResponse, Response as HttpResponse
from starlette.routing import Route
from starlette.requests import Request
from datetime import datetime
import asyncpg
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import pydantic


load_dotenv()

import orjson
from starlette.responses import JSONResponse


def default(obj: Any) -> dict[str, Any]:
    if isinstance(obj, pydantic.BaseModel):
        return obj.dict()
    raise TypeError


class OrjsonResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, default=default)


connection_pool: asyncpg.Pool[asyncpg.Record]


class Ingredient(pydantic.BaseModel):
    id: int
    position: str
    quantity: str
    name: str
    description: str


class Step(pydantic.BaseModel):
    id: int
    position: str
    text: str


class Reaction(pydantic.BaseModel):
    id: int
    emoji: str
    created_by_id: int


class Note(pydantic.BaseModel):
    id: int
    text: str
    email: str
    name: str | None
    modified_at: datetime
    created_at: datetime
    reactions: list[Reaction]


class Section(pydantic.BaseModel):
    id: int
    title: str
    position: str


class TimelineEvent(pydantic.BaseModel):
    id: int
    action: str
    created_at: datetime
    created_by_id: int | None
    created_by_name: str | None


class Recipe(pydantic.BaseModel):
    id: int
    name: str
    author: str
    source: str
    time: str
    servings: str
    tags: list[str]
    archived_at: datetime | None
    created_at: datetime
    ingredients: list[Ingredient | Section]
    steps: list[Step]
    timeline: list[TimelineEvent | Note]


async def homepage(request: Request) -> HttpResponse:
    # TODO: make this a detail request
    session = request.cookies["sessionid"]
    if not session:
        return HttpResponse(status_code=401)
    async with connection_pool.acquire() as connection:
        await connection.execute("SET TIME ZONE 'UTC'")
        now_utc = datetime.now(timezone.utc)
        maybe_session = await connection.fetchrow(
            """
SELECT
	"user_sessions_session"."user_id"
FROM
	"user_sessions_session"
WHERE ("user_sessions_session"."expire_date" > $2::timestamptz
	AND "user_sessions_session"."session_key" = $1
    )
LIMIT 1;""",
            session,
            now_utc,
        )
        if maybe_session is None:
            return OrjsonResponse({"error": "not authed"}, status_code=401)

        user_id = maybe_session["user_id"]

        limit = 1

        recipes = await connection.fetch(
            """
 SELECT
	"core_recipe"."id",
	"core_recipe"."name",
	"core_recipe"."author",
	"core_recipe"."source",
	"core_recipe"."time",
	"core_recipe"."servings",
	"core_recipe"."edits",
	"core_recipe"."modified",
	"core_team"."id" "team_id",
	"core_team"."name",
	"core_myuser"."id" "user_id",
	"core_recipe"."created",
	"core_recipe"."archived_at",
	"core_recipe"."tags"
FROM
	"core_recipe"
	LEFT OUTER JOIN "core_myuser" ON ("core_recipe"."object_id" = "core_myuser"."id"
		AND("core_recipe"."content_type_id" = 1))
	LEFT OUTER JOIN "core_team" ON ("core_recipe"."object_id" = "core_team"."id"
		AND("core_recipe"."content_type_id" = 20))
WHERE ("core_recipe"."deleted_at" IS NULL
	AND("core_myuser"."id" = $1
		OR "core_team"."id" IN(
			SELECT
				U0. "team_id" FROM "core_membership" U0
			WHERE (U0. "user_id" = $1
				AND U0. "is_active"))))
               order by random() -- hacky solution to get a random recipe to simulate a detail view
               limit $2

                ;


        """,
            user_id,
            limit,
        )

        recipe_ids = [r["id"] for r in recipes]

        recipe = recipes[0]

        ingredient_rows = await connection.fetch(
            """
SELECT
	"core_ingredient"."id",
	"core_ingredient"."position",
	"core_ingredient"."quantity",
	"core_ingredient"."name",
	"core_ingredient"."description"
FROM
	"core_ingredient"
WHERE ("core_ingredient"."deleted_at" IS NULL
	AND "core_ingredient"."recipe_id" = any($1::int[]) )
ORDER BY
	"core_ingredient"."position" ASC;

        """,
            recipe_ids,
        )

        step_rows = await connection.fetch(
            """
SELECT
	"core_step"."id",
	"core_step"."text",
	"core_step"."position",
	"core_step"."recipe_id"
FROM
	"core_step"
WHERE ("core_step"."deleted_at" IS NULL
	AND "core_step"."recipe_id" = any($1::int[]) )
ORDER BY
	"core_step"."position" ASC;
        """,
            recipe_ids,
        )

        section_rows = await connection.fetch(
            """
SELECT
	"core_section"."id",
	"core_section"."title",
	"core_section"."position",
	"core_section"."recipe_id"
FROM
	"core_section"
WHERE ("core_section"."deleted_at" IS NULL
	AND "core_section"."recipe_id" = any($1::int[]))
ORDER BY
	"core_section"."position" ASC;
""",
            recipe_ids,
        )

        note_rows = await connection.fetch(
            """
SELECT
	"core_note"."id",
	"core_note"."text",
	"core_note"."modified",
	"core_note"."created",
	"core_note"."recipe_id",
	"core_note"."last_modified_by_id",
	"core_myuser"."email",
	"core_myuser"."name",
	"core_note"."created_by_id",
	T4. "email",
	T4. "name"
FROM
	"core_note"
	LEFT OUTER JOIN "core_myuser" ON ("core_note"."last_modified_by_id" = "core_myuser"."id")
INNER JOIN "core_myuser" T4 ON ("core_note"."created_by_id" = T4. "id")
WHERE ("core_note"."deleted_at" IS NULL
	AND "core_note"."recipe_id" = any($1::int[]))
ORDER BY
	"core_note"."created" DESC;

        """,
            recipe_ids,
        )

        reaction_rows = await connection.fetch(
            """
SELECT
	"core_reaction"."id",
	"core_reaction"."created",
	"core_reaction"."modified",
	"core_reaction"."emoji",
	"core_reaction"."created_by_id",
	"core_reaction"."note_id"
FROM
	"core_reaction"
	INNER JOIN "core_note" ON ("core_reaction"."note_id" = "core_note"."id")
WHERE
	"core_note"."recipe_id" = any($1::int[])
ORDER BY
	"core_reaction"."created" DESC;
        """,
            recipe_ids,
        )

        timeline_rows: list[asyncpg.Record] = await connection.fetch(
            """
SELECT
	"timeline_event"."id",
	"timeline_event"."action",
	"timeline_event"."created",
	"timeline_event"."created_by_id",
	"core_myuser"."email"
FROM
	"timeline_event"
	LEFT OUTER JOIN "core_myuser" ON ("timeline_event"."created_by_id" = "core_myuser"."id")
WHERE ("timeline_event"."deleted_at" IS NULL
	AND "timeline_event"."recipe_id" = any($1::int[]))
ORDER BY
	"timeline_event"."created" DESC;

        """,
            recipe_ids,
        )

    ingredients: list[Section | Ingredient] = []
    for i in ingredient_rows:
        ingredients.append(
            Ingredient(
                id=i["id"],
                position=i["position"],
                quantity=i["quantity"],
                name=i["name"],
                description=i["description"],
            )
        )
    for sec in section_rows:
        ingredients.append(
            Section(id=sec["id"], title=sec["title"], position=sec["position"])
        )

    steps = [
        Step(id=s["id"], position=s["position"], text=s["text"]) for s in step_rows
    ]

    reactions: dict[int, list[Reaction]] = defaultdict(list)
    for r in reaction_rows:
        reactions[r["note_id"]].append(
            Reaction(id=r["id"], emoji=r["emoji"], created_by_id=r["created_by_id"])
        )

    timeline: list[TimelineEvent | Note] = []
    for t in timeline_rows:
        timeline.append(
            TimelineEvent(
                id=t["id"],
                action=t["action"],
                created_at=t["created"],
                created_by_id=t["created_by_id"],
                created_by_name=t["email"],
            )
        )
    for n in note_rows:
        timeline.append(
            Note(
                id=n["id"],
                text=n["text"],
                email=n["email"],
                name=n["name"],
                modified_at=n["modified"],
                created_at=n["created"],
                reactions=reactions[n["id"]],
            )
        )

    return OrjsonResponse(
        Recipe(
            id=recipe["id"],
            name=recipe["name"],
            author=recipe["author"],
            source=recipe["source"],
            time=recipe["time"],
            servings=recipe["servings"],
            tags=recipe["tags"],
            archived_at=recipe["archived_at"],
            created_at=recipe["created"],
            ingredients=ingredients,
            steps=steps,
            timeline=timeline,
        )
    )


async def setup_database() -> None:
    global connection_pool

    connection_pool = await asyncpg.create_pool(  # type: ignore
        dsn=os.environ["PG_DSN"],
    )


app = Starlette(
    routes=[
        Route("/api/v1/recipes", homepage),
    ],
    on_startup=[setup_database],
)
