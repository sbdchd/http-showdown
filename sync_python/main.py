from datetime import datetime, timezone
import os
import sys
from typing import Any
from collections import defaultdict

from django.conf import settings
from django.core.wsgi import get_wsgi_application
from django.urls import path
from django.utils.crypto import get_random_string
from django.http.request import HttpRequest
from django.http.response import HttpResponse
from django.db import connection
import dj_database_url
import orjson
import pydantic

from dotenv import load_dotenv


load_dotenv()


settings.configure(
    DEBUG=False,
    ALLOWED_HOSTS=["*"],
    ROOT_URLCONF=__name__,
    SECRET_KEY=get_random_string(
        50
    ),
    MIDDLEWARE=["django.middleware.common.CommonMiddleware"],
    DATABASES={"default": dj_database_url.parse(os.environ["PG_DSN"])},
)


def default(obj: Any) -> dict[str, Any]:
    if isinstance(obj, pydantic.BaseModel):
        return obj.dict()
    raise TypeError


class OrJsonResponse(HttpResponse):
    def __init__(
        self,
        data: Any,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("content_type", "application/json")
        data = orjson.dumps(data, default=default)
        super().__init__(content=data, **kwargs)


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
    created_by_id: str


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


def dictfetchall(cursor: Any) -> list[dict[str, Any]]:
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def recipes_list(request: HttpRequest) -> HttpResponse:
    session = request.COOKIES["sessionid"]
    if not session:
        return HttpResponse(status_code=401)
    with connection.cursor() as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        now_utc = datetime.now(timezone.utc)
        conn.execute(
            """
SELECT
	"user_sessions_session"."user_id"
FROM
	"user_sessions_session"
WHERE ("user_sessions_session"."expire_date" > %(now_utc)s::timestamptz
	AND "user_sessions_session"."session_key" = %(session)s
    )
LIMIT 1;
""",
            {"session": session, "now_utc": now_utc},
        )
        maybe_session = conn.fetchall()
        if not maybe_session:
            return OrJsonResponse({"error": "not authed"}, status_code=401)

        (user_id,) = maybe_session[0]

        limit = 1

        conn.execute(
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
	AND("core_myuser"."id" = %(user_id)s::int
		OR "core_team"."id" IN(
			SELECT
				U0. "team_id" FROM "core_membership" U0
			WHERE (U0. "user_id" = %(user_id)s::int
				AND U0. "is_active"))))

order by random() -- hacky solution to get a random recipe to simulate a detail view

limit %(limit)s

                ;
        """,
            {"user_id": str(user_id), "limit": limit},
        )
        recipes = dictfetchall(conn)

        recipe_ids = [r["id"] for r in recipes]

        recipe = recipes[0]

        conn.execute(
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
	AND "core_ingredient"."recipe_id" in %(recipe_ids)s )
ORDER BY
	"core_ingredient"."position" ASC;

        """,
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        ingredient_rows = dictfetchall(conn)
        conn.execute(
            """
SELECT
	"core_step"."id",
	"core_step"."text",
	"core_step"."position",
	"core_step"."recipe_id"
FROM
	"core_step"
WHERE ("core_step"."deleted_at" IS NULL
	AND "core_step"."recipe_id" in %(recipe_ids)s )
ORDER BY
	"core_step"."position" ASC;
        """,
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        step_rows = dictfetchall(conn)
        conn.execute(
            """
SELECT
	"core_section"."id",
	"core_section"."title",
	"core_section"."position",
	"core_section"."recipe_id"
FROM
	"core_section"
WHERE ("core_section"."deleted_at" IS NULL
	AND "core_section"."recipe_id" in %(recipe_ids)s)
ORDER BY
	"core_section"."position" ASC;
""",
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        section_rows = dictfetchall(conn)

        conn.execute(
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
	AND "core_note"."recipe_id" in %(recipe_ids)s)
ORDER BY
	"core_note"."created" DESC;

        """,
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        note_rows = dictfetchall(conn)

        conn.execute(
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
	"core_note"."recipe_id" in %(recipe_ids)s
ORDER BY
	"core_reaction"."created" DESC;
        """,
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        reaction_rows = dictfetchall(conn)

        conn.execute(
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
	AND "timeline_event"."recipe_id" in %(recipe_ids)s )
ORDER BY
	"timeline_event"."created" DESC;

        """,
            {
                "recipe_ids": tuple(recipe_ids),
            },
        )
        timeline_rows = dictfetchall(conn)

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

    return OrJsonResponse(
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


urlpatterns = [
    path("api/v1/recipes", recipes_list),
]

app = get_wsgi_application()

if __name__ == "__main__":
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
