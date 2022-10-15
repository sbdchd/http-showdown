use axum::{extract::Extension, http::StatusCode, routing::get, Json, Router};
use axum_extra::extract::cookie::CookieJar;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::Utc;
use dotenvy::dotenv;
use http::Request;
use hyper::Body;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tower_request_id::{RequestId, RequestIdLayer};
use tracing::{info, info_span, Level};

use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let dsn = env::var("PG_DSN").unwrap();

    let cert = fs::read("database_cert.pem").unwrap();
    let cert = Certificate::from_pem(&cert).unwrap();
    let connector = TlsConnector::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let connector = MakeTlsConnector::new(connector);

    let manager = PostgresConnectionManager::new_from_stringlike(dsn, connector)
        .expect("setup conn manager, whatever that is");
    let pool = Pool::builder()
        .max_size(20)
        .build(manager)
        .await
        .expect("created pool successfully");

    let app = Router::new()
        .route("/api/v1/recipes", get(recipes_list))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<Body>| {
                // taken from: https://github.com/imbolc/tower-request-id/blob/1171b95f15ba5a3456b0425cbc0c4d486444ceaf/examples/logging.rs
                let request_id = request
                    .extensions()
                    .get::<RequestId>()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown".into());
                // HACK: get some logging, not sure how to get spans to show up
                info!(
                    "request {id} {method} {uri}",
                    id = request_id,
                    method = request.method(),
                    uri = request.uri(),
                );
                info_span!(
                    "request",
                    id = %request_id,
                    method = %request.method(),
                    uri = %request.uri(),
                )
            }),
        )
        .layer(RequestIdLayer)
        .layer(Extension(pool));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

type ConnectionPool = Pool<PostgresConnectionManager<MakeTlsConnector>>;

#[derive(Serialize, Default)]
struct Ingredient {
    id: i32,
    position: String,
    quantity: String,
    name: String,
    description: String,
}

#[derive(Serialize, Default)]
struct Step {
    id: i32,
    position: String,
    text: String,
}

#[derive(Serialize, Clone, Default, Debug)]
struct Reaction {
    id: i32,
    emoji: String,
    created_by_id: i32,
}

#[derive(Serialize, Default)]
struct Note {
    id: i32,
    text: String,
    email: Option<String>,
    name: Option<String>,
    modified_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
    reactions: Vec<Reaction>,
}

#[derive(Serialize, Default)]
struct Section {
    id: i32,
    title: String,
    position: String,
}

#[derive(Serialize, Default)]
struct TimelineEvent {
    id: i32,
    action: String,
    created_at: chrono::DateTime<Utc>,
    created_by_id: Option<i32>,
    created_by_name: Option<String>,
}

#[derive(Serialize)]
enum IngredientLike {
    Ingredient(Ingredient),
    Section(Section),
}

#[derive(Serialize)]
enum TimelineLike {
    TimelineEvent(TimelineEvent),
    Note(Note),
}

#[derive(Serialize, Default)]
struct Recipe {
    id: i32,
    name: String,
    author: Option<String>,
    source: Option<String>,
    time: String,
    servings: String,
    tags: Vec<String>,
    archived_at: Option<chrono::DateTime<Utc>>,
    created_at: Option<chrono::DateTime<Utc>>,
    ingredients: Vec<IngredientLike>,
    steps: Vec<Step>,
    timeline: Vec<TimelineLike>,
}

// basic handler that responds with a static string
async fn recipes_list(
    Extension(pool): Extension<ConnectionPool>,
    jar: CookieJar,
) -> Result<Json<Recipe>, (StatusCode, String)> {
    let session_id = jar
        .get("sessionid")
        .map(|cookie| cookie.value().to_owned())
        .ok_or((StatusCode::UNAUTHORIZED, "problem parsing session".into()))?;

    tracing::debug!("getting conn...");

    let conn = pool
        .get()
        .await
        .map_err(|_err| (StatusCode::INTERNAL_SERVER_ERROR, "foo".into()))?;

    tracing::debug!("conn done");
    conn.execute("SET TIME ZONE 'UTC'", &[])
        .await
        .map_err(internal_error)?;

    let now_utc = Utc::now();
    tracing::debug!("conn done");

    let maybe_session = conn
        .query_one(
            r#"
SELECT
	"user_sessions_session"."user_id"
FROM
	"user_sessions_session"
WHERE ("user_sessions_session"."expire_date" > $2::timestamptz
	AND "user_sessions_session"."session_key" = $1
    )
LIMIT 1;"#,
            // hit    |                            ^^^^^^^ expected `&dyn ToSql + Sync`, found struct `chrono::DateTime<Utc>`
            // needed to add features = ["with-chrono-0_4"]
            &[&session_id, &now_utc],
        )
        .await
        .map_err(internal_error)?;

    let user_id: i32 = maybe_session
        .try_get("user_id")
        .map_err(|_err| (StatusCode::UNAUTHORIZED, "unauthorized".into()))?;

    let limit: i64 = 1;

    let recipes = conn
        .query(
            r#"
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
        "#,
            &[&user_id, &limit],
        )
        .await
        .map_err(internal_error)?;

    let recipe_ids: Vec<i32> = recipes.iter().map(|r| r.get("id")).collect();

    let ingredient_rows = conn
        .query(
            r#"
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
        "#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let step_rows = conn
        .query(
            r#"
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
        "#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let section_rows = conn
        .query(
            r#"
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
"#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let note_rows = conn
        .query(
            r#"
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

        "#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let reaction_rows = conn
        .query(
            r#"
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
        "#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let timeline_rows = conn
        .query(
            r#"
SELECT
	"timeline_event"."id",
	"timeline_event"."action",
	"timeline_event"."created",
	"timeline_event"."created_by_id",
	"core_myuser"."email",
	"timeline_event"."recipe_id"
FROM
	"timeline_event"
	LEFT OUTER JOIN "core_myuser" ON ("timeline_event"."created_by_id" = "core_myuser"."id")
WHERE ("timeline_event"."deleted_at" IS NULL
	AND "timeline_event"."recipe_id" = any($1::int[]))
ORDER BY
	"timeline_event"."created" DESC;

        "#,
            &[&recipe_ids],
        )
        .await
        .map_err(internal_error)?;

    let mut ingredients = vec![];
    for i in ingredient_rows {
        ingredients.push(IngredientLike::Ingredient(Ingredient {
            id: i.get("id"),
            position: i.get("position"),
            quantity: i.get("quantity"),
            name: i.get("name"),
            description: i.get("description"),
        }))
    }
    for sec in section_rows {
        ingredients.push(IngredientLike::Section(Section {
            id: sec.get("id"),
            title: sec.get("title"),
            position: sec.get("position"),
        }))
    }

    let steps = step_rows
        .into_iter()
        .map(|s| Step {
            id: s.get("id"),
            position: s.get("position"),
            text: s.get("text"),
        })
        .collect();

    let mut reactions: HashMap<i32, Vec<Reaction>> = HashMap::new();
    for r in reaction_rows {
        reactions
            .entry(r.get("note_id"))
            .or_insert_with(|| vec![])
            .push(Reaction {
                id: r.get("id"),
                emoji: r.get("emoji"),
                created_by_id: r.get("created_by_id"),
            });
    }

    let mut timeline: Vec<TimelineLike> = vec![];
    for t in timeline_rows {
        timeline.push(TimelineLike::TimelineEvent(TimelineEvent {
            id: t.get("id"),
            action: t.get("action"),
            created_at: t.get("created"),
            created_by_id: t.get("created_by_id"),
            created_by_name: t.get("email"),
        }))
    }
    for n in note_rows {
        timeline.push(TimelineLike::Note(Note {
            id: n.get("id"),
            text: n.get("text"),
            email: n.get("email"),
            name: n.get("name"),
            modified_at: n.get("modified"),
            created_at: n.get("created"),
            reactions: reactions.entry(n.get("id")).or_default().clone(),
        }))
    }

    let recipe = &recipes[0];
    return Ok(Json(Recipe {
        id: recipe.get("id"),
        name: recipe.get("name"),
        author: recipe.get("author"),
        source: recipe.get("source"),
        time: recipe.get("time"),
        servings: recipe.get("servings"),
        tags: recipe.get("tags"),
        archived_at: recipe.get("archived_at"),
        created_at: recipe.get("created"),
        ingredients,
        steps,
        timeline,
    }));
}

/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
