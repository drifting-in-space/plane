use super::{core::Controller, error::IntoApiError};
use crate::types::{ClusterName, ClusterState};
use axum::{
    extract::{Path, State},
    response::Response,
    Json,
};

pub async fn handle_cluster_state(
    Path(cluster_name): Path<ClusterName>,
    State(controller): State<Controller>,
) -> Result<Json<ClusterState>, Response> {
    let result = controller
        .db
        .cluster()
        .cluster_state(&cluster_name)
        .await
        .or_internal_error("Database error")?;

    Ok(Json(result))
}
